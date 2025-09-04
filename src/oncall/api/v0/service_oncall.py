# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.


import time
from ujson import dumps as json_dumps
from ... import db


def _ph(n: int) -> str:
    return ", ".join(["%s"] * n)


def _chunks(seq, size=1000):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def on_get(req, resp, service, role=None):
    '''
    Get the current user on-call for a given service/role. Returns event start/end, contact info,
    and user name.

    **Example request**

    .. sourcecode:: http

        GET /api/v0/services/service-foo/oncall/primary  HTTP/1.1
        Host: example.com


    **Example response**:

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        [
            {
                "contacts": {
                    "call": "+1 111-111-1111",
                    "email": "jdoe@example.com",
                    "im": "jdoe",
                    "sms": "+1 111-111-1111"
                },
                "end": 1495695600,
                "start": 1495263600,
                "user": "John Doe"
            }
        ]

    '''
    now = int(time.time())
    conn = db.connect()
    cur = conn.cursor(db.DictCursor)

    # 1) Resolve owning teams and overrides for the service
    cur.execute(
        """
        SELECT ts.team_id,
               t.name,
               t.override_phone_number
        FROM team_service ts
        JOIN service s ON s.id = ts.service_id
        JOIN team t ON t.id = ts.team_id
        WHERE s.name = %s
        """,
        (service,),
    )
    rows = cur.fetchall()
    if not rows:
        resp.body = json_dumps([])
        cur.close()
        conn.close()
        return

    owning_team_ids = [r["team_id"] for r in rows]
    # Map team name -> override phone number (parity with original code)
    team_override_numbers = {r["name"]: r["override_phone_number"] for r in rows}

    # Optional role filter
    role_id = None
    if role is not None:
        cur.execute("SELECT id FROM role WHERE name = %s", (role,))
        rr = cur.fetchone()
        if not rr:
            resp.body = json_dumps([])
            cur.close()
            conn.close()
            return
        role_id = rr["id"]

    # 2A) Active events on owning teams (uses idx_event_team_start_role)
    events_a = []
    for chunk in _chunks(owning_team_ids):
        pred = f"team_id IN ({_ph(len(chunk))}) AND start <= %s AND end >= %s"
        params = list(chunk) + [now, now]
        if role_id is not None:
            pred += " AND role_id = %s"
            params.append(role_id)

        cur.execute(
            f"""
            SELECT id, team_id, user_id, role_id, start, end
            FROM event
            WHERE {pred}
            """,
            params,
        )
        events_a.extend(cur.fetchall())

    # 2B) Active events from teams that the owners subscribe to (role-scoped)
    # Get (subscription_id, role_id) pairs for all owning teams
    if role_id is not None:
        # Only this role
        pairs = []
        for chunk in _chunks(owning_team_ids):
            cur.execute(
                f"""
                SELECT subscription_id AS team_id, role_id
                FROM team_subscription
                WHERE team_id IN ({_ph(len(chunk))}) AND role_id = %s
                """,
                list(chunk) + [role_id],
            )
            pairs.extend([(r["team_id"], r["role_id"]) for r in cur.fetchall()])
    else:
        pairs = []
        for chunk in _chunks(owning_team_ids):
            cur.execute(
                f"""
                SELECT subscription_id AS team_id, role_id
                FROM team_subscription
                WHERE team_id IN ({_ph(len(chunk))})
                """,
                chunk,
            )
            pairs.extend([(r["team_id"], r["role_id"]) for r in cur.fetchall()])

    events_b = []
    if pairs:
        # Query events by (team_id, role_id) pairs to match legacy JOIN condition exactly
        for chunk in _chunks(pairs, size=500):
            tuple_placeholders = ", ".join(["(%s, %s)"] * len(chunk))
            flat = [v for tid_rid in chunk for v in tid_rid]
            cur.execute(
                f"""
                SELECT id, team_id, user_id, role_id, start, end
                FROM event
                WHERE (team_id, role_id) IN ({tuple_placeholders})
                  AND start <= %s AND end >= %s
                """,
                flat + [now, now],
            )
            events_b.extend(cur.fetchall())

    # 3) Merge A then B (branch order preserves "first event per user" semantics)
    evs = events_a + events_b
    if not evs:
        resp.body = json_dumps([])
        cur.close()
        conn.close()
        return

    # 4) Fetch user/team/role names and contacts only for the small result set
    user_ids = sorted({e["user_id"] for e in evs})
    team_ids = sorted({e["team_id"] for e in evs})
    role_ids = sorted({e["role_id"] for e in evs})

    users = {}
    for chunk in _chunks(user_ids):
        cur.execute(
            f"SELECT id, name, full_name FROM user WHERE id IN ({_ph(len(chunk))})",
            chunk,
        )
        for r in cur.fetchall():
            users[r["id"]] = (r["name"], r["full_name"])

    contacts_by_user = {}
    for chunk in _chunks(user_ids):
        cur.execute(
            f"""
            SELECT uc.user_id, cm.name AS mode, uc.destination
            FROM user_contact uc
            JOIN contact_mode cm ON cm.id = uc.mode_id
            WHERE uc.user_id IN ({_ph(len(chunk))})
            """,
            chunk,
        )
        for r in cur.fetchall():
            d = contacts_by_user.setdefault(r["user_id"], {})
            d[r["mode"]] = r["destination"]

    team_name = {}
    for chunk in _chunks(team_ids):
        cur.execute(
            f"SELECT id, name FROM team WHERE id IN ({_ph(len(chunk))})",
            chunk,
        )
        for r in cur.fetchall():
            team_name[r["id"]] = r["name"]

    role_name = {}
    for chunk in _chunks(role_ids):
        cur.execute(
            f"SELECT id, name FROM role WHERE id IN ({_ph(len(chunk))})",
            chunk,
        )
        for r in cur.fetchall():
            role_name[r["id"]] = r["name"]

    # 5) Assemble: first event per user wins (exact parity)
    seen = set()
    out = []
    for e in evs:
        uid = e["user_id"]
        if uid in seen:
            continue
        seen.add(uid)

        uname, full = users.get(uid, (None, None))
        if uname is None:
            continue

        item = {
            "contacts": contacts_by_user.get(uid, {}).copy(),
            "end": e["end"],
            "start": e["start"],
            "user": uname,               # parity with original: 'user' is user.name
            "full_name": full,           # still present (selected in legacy)
            "team": team_name.get(e["team_id"]),
            "role": role_name.get(e["role_id"]),
        }
        out.append(item)

    # 6) Apply team override for primary role (parity)
    for event in out:
        override_number = team_override_numbers.get(event["team"])
        if override_number and event.get("role") == "primary":
            event["contacts"]["call"] = override_number
            event["contacts"]["sms"] = override_number

    cur.close()
    conn.close()
    resp.body = json_dumps(out)
