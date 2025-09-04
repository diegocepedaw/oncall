# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import time
from ujson import dumps as json_dumps
from ... import db


def _expand_in_placeholders(n: int) -> str:
    # returns "%s, %s, ... %s" of length n (n >= 1)
    return ", ".join(["%s"] * n)


def _chunk(seq, size):
    it = iter(seq)
    while True:
        buf = []
        try:
            for _ in range(size):
                buf.append(next(it))
        except StopIteration:
            if buf:
                yield buf
            break
        yield buf


def on_get(req, resp, team, role=None):
    """
    Get current active event for team based on given role.

    **Example request**:

    .. sourcecode:: http

       GET /api/v0/teams/team_ops/oncall/primary HTTP/1.1
       Host: example.com

    **Example response**:

    .. sourcecode:: http

       HTTP/1.1 200 OK
       Content-Type: application/json

       [
         {
           "user": "foo",
           "start": 1487426400,
           "end": 1487469600,
           "full_name": "Foo Icecream",
           "contacts": {
             "im": "foo",
             "sms": "+1 123-456-7890",
             "email": "foo@example.com",
             "call": "+1 123-456-7890"
           }
         },
         {
           "user": "bar",
           "start": 1487426400,
           "end": 1487469600,
           "full_name": "Bar Dog",
           "contacts": {
             "im": "bar",
             "sms": "+1 123-456-7890",
             "email": "bar@example.com",
             "call": "+1 123-456-7890"
           }
         }
       ]

    :statuscode 200: no error
    """

    now = int(time.time())

    conn = db.connect()
    cur = conn.cursor(db.DictCursor)

    # 1) Resolve team_id, override number, and optional role_id up front
    cur.execute(
        "SELECT id, override_phone_number FROM team WHERE name = %s",
        (team,),
    )
    row = cur.fetchone()
    if not row:
        # Team not found -> empty result
        resp.body = json_dumps([])
        cur.close()
        conn.close()
        return

    team_id = row["id"]
    override_number = row["override_phone_number"]

    role_id = None
    if role is not None:
        # role.name should be unique; use that index
        cur.execute("SELECT id FROM role WHERE name = %s", (role,))
        r = cur.fetchone()
        if not r:
            # Unknown role -> empty result
            resp.body = json_dumps([])
            cur.close()
            conn.close()
            return
        role_id = r["id"]

    # 2) Active events directly on the requested team
    # Uses idx_event_team_start_role(team_id, start, role_id)
    params = [team_id, now, now]
    pred = "team_id = %s AND start <= %s AND end >= %s"
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
    events = list(cur.fetchall())

    # 3) Active events from subscribed teams (role-scoped subscriptions)
    # First get the subscriptions. Plan to batch IN lists to protect the planner.
    if role_id is not None:
        cur.execute(
            """
            SELECT subscription_id AS team_id, role_id
            FROM team_subscription
            WHERE team_id = %s AND role_id = %s
            """,
            (team_id, role_id),
        )
    else:
        cur.execute(
            """
            SELECT subscription_id AS team_id, role_id
            FROM team_subscription
            WHERE team_id = %s
            """,
            (team_id,),
        )
    subs = list(cur.fetchall())
    sub_team_ids = [s["team_id"] for s in subs]
    # Optional: map role limits per subscription row if needed later
    # but we only need team list + role filter (if provided).

    if sub_team_ids:
        # Build predicate once to leverage idx_event_team_start_role
        base_pred = "start <= %s AND end >= %s"
        base_params = [now, now]

        if role_id is not None:
            base_pred += " AND role_id = %s"
            base_params.append(role_id)

        # Batch IN clauses to avoid huge lists
        for chunk in _chunk(sub_team_ids, 1000):
            placeholders = _expand_in_placeholders(len(chunk))
            cur.execute(
                f"""
                SELECT id, team_id, user_id, role_id, start, end
                FROM event
                WHERE team_id IN ({placeholders}) AND {base_pred}
                """,
                chunk + base_params,
            )
            events.extend(cur.fetchall())

    if not events:
        # No active events at all
        resp.body = json_dumps([])
        cur.close()
        conn.close()
        return

    # 4) Fetch users, contacts, and team names for the small result set

    user_ids = sorted({e["user_id"] for e in events})
    team_ids = sorted({e["team_id"] for e in events})

    # Users
    users = {}
    for chunk in _chunk(user_ids, 1000):
        ph = _expand_in_placeholders(len(chunk))
        cur.execute(
            f"""
            SELECT id, name, full_name
            FROM user
            WHERE id IN ({ph})
            """,
            chunk,
        )
        for r in cur.fetchall():
            users[r["id"]] = {"name": r["name"], "full_name": r["full_name"]}

    # Contacts (join to contact_mode after shrinking user_id set)
    contacts_by_user = {}
    for chunk in _chunk(user_ids, 1000):
        ph = _expand_in_placeholders(len(chunk))
        cur.execute(
            f"""
            SELECT uc.user_id, cm.name AS mode, uc.destination
            FROM user_contact uc
            JOIN contact_mode cm ON cm.id = uc.mode_id
            WHERE uc.user_id IN ({ph})
            """,
            chunk,
        )
        for r in cur.fetchall():
            u = r["user_id"]
            contacts_by_user.setdefault(u, {})[r["mode"]] = r["destination"]

    # Team names for all event teams (including subscriptions)
    team_name_by_id = {}
    for chunk in _chunk(team_ids, 1000):
        ph = _expand_in_placeholders(len(chunk))
        cur.execute(
            f"""
            SELECT id, name
            FROM team
            WHERE id IN ({ph})
            """,
            chunk,
        )
        for r in cur.fetchall():
            team_name_by_id[r["id"]] = r["name"]

    # Role names (small domain; if role is provided we still map id->name)
    role_ids = sorted({e["role_id"] for e in events})
    role_name_by_id = {}
    for chunk in _chunk(role_ids, 1000):
        ph = _expand_in_placeholders(len(chunk))
        cur.execute(
            f"""
            SELECT id, name
            FROM role
            WHERE id IN ({ph})
            """,
            chunk,
        )
        for r in cur.fetchall():
            role_name_by_id[r["id"]] = r["name"]

    # 5) Assemble results and fold contact modes by user
    # The legacy shape is one object per unique user (latest event window kept as seen)
    by_user = {}
    for e in events:
        uid = e["user_id"]
        u = users.get(uid)
        if not u:
            # Shouldn't happen, but guard anyway
            continue
        item = by_user.get(uid)
        if item is None:
            item = {
                "user": u["name"],
                "full_name": u["full_name"],
                "start": e["start"],
                "end": e["end"],
                "team": team_name_by_id.get(e["team_id"]),
                "role": role_name_by_id.get(e["role_id"]),
                "contacts": contacts_by_user.get(uid, {}).copy(),
            }
            by_user[uid] = item
        else:
            # If the same user appears multiple times, keep the widest window
            if e["start"] < item["start"]:
                item["start"] = e["start"]
            if e["end"] > item["end"]:
                item["end"] = e["end"]
            # Contacts already folded

    data = list(by_user.values())

    # Primary override for requested team only (matches old semantics)
    if override_number:
        for ev in data:
            if ev["role"] == "primary":
                ev["contacts"]["call"] = override_number
                ev["contacts"]["sms"] = override_number

    cur.close()
    conn.close()

    resp.body = json_dumps(data)
