# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from testutils import prefix, api_v0
import time
import requests


@prefix('test_v0_populate_new')
def test_api_v0_populate_new(user, team, roster, role, schedule):
    user_name = user.create()
    user_name_2 = user.create()
    team_name = team.create()
    role_name = role.create()
    start__lt = time.time() + 31540000
    end__ge = 0
    team__eq = team_name
    start = time.time()
    roster_name = roster.create(team_name)
    schedule_id = schedule.create(team_name,
                                  roster_name,
                                  {'role': role_name,
                                   'events': [{'start': 0, 'duration': 604800}],
                                   'advanced_mode': 0,
                                   'auto_populate_threshold': 14})
    user.add_to_roster(user_name, team_name, roster_name)
    user.add_to_roster(user_name_2, team_name, roster_name)

    def clean_up():
        re = requests.get(api_v0('events?team='+team_name))
        for ev in re.json():
            requests.delete(api_v0('events/%d' % ev['id']))

    clean_up()

    # also test that the events generated by preview match the events generated by populate
    re = requests.get(api_v0('schedules/%s/preview?team__eq=%s&start=%s&teamName=%s&end__ge=%s&start__lt=%s' % (schedule_id, team__eq, start, team_name, end__ge, start__lt)))
    assert re.status_code == 200
    preview_events = re.json()

    re = requests.post(api_v0('schedules/%s/populate' % schedule_id), json = {'start': start})
    assert re.status_code == 200

    re = requests.get(api_v0('events?team=%s' % team_name))
    assert re.status_code == 200
    events = re.json()

    for ev in events:
        ev['id'] = None
    for ev in preview_events:
        ev['id'] = None

    assert events == preview_events
    assert len(events) == 2
    users = set([ev['user'] for ev in events])
    assert user_name in users
    assert user_name_2 in users

    clean_up()


@prefix('test_v0_populate_vacation_propagate')
def test_v0_populate_vacation_propagate(user, team, roster, role, schedule, event):
    user_name = user.create()
    user_name_2 = user.create()
    team_name = team.create()
    team_name_2 = team.create()
    role_name = role.create()
    roster_name = roster.create(team_name)
    schedule_id = schedule.create(team_name,
                                  roster_name,
                                  {'role': role_name,
                                   'events': [{'start': 0, 'duration': 604800}],
                                   'advanced_mode': 0,
                                   'auto_populate_threshold': 14})
    user.add_to_roster(user_name, team_name, roster_name)
    user.add_to_roster(user_name_2, team_name, roster_name)
    user.add_to_team(user_name, team_name_2)

    # Populate for team 1
    re = requests.post(api_v0('schedules/%s/populate' % schedule_id), json = {'start': time.time()})
    assert re.status_code == 200

    # Create conflicting vacation event in team 2 for user 1
    re = requests.get(api_v0('events?team=%s' % team_name))
    assert re.status_code == 200
    events = re.json()
    assert len(events) == 2
    assert events[0]['user'] != events[1]['user']
    for e in events:
        event.create({
            'start': e['start'],
            'end': e['end'],
            'user': user_name,
            'team': team_name_2,
            'role': "vacation",
        })

    # Populate again for team 1
    re = requests.post(api_v0('schedules/%s/populate' % schedule_id), json = {'start': time.time()})
    assert re.status_code == 200

    # Ensure events are both for user 2 (since user 1 is busy in team 2)
    re = requests.get(api_v0('events?team=%s&include_subscribed=false' % team_name))
    assert re.status_code == 200
    events = re.json()
    assert len(events) == 2
    assert events[0]['user'] == events[1]['user'] == user_name_2


@prefix('test_v0_populate_vacation_propagate')
def test_v0_populate_multi_team(user, team, roster, role, schedule, event):
    user_name = user.create()
    user_name_2 = user.create()
    team_name = team.create()
    team_name_2 = team.create()
    role_name = role.create()
    roster_name = roster.create(team_name)
    schedule_id = schedule.create(team_name,
                                  roster_name,
                                  {'role': role_name,
                                   'events': [{'start': 0, 'duration': 604800}],
                                   'advanced_mode': 0,
                                   'auto_populate_threshold': 14,
                                   'scheduler': {'name': 'multi-team', 'data': []}})
    user.add_to_roster(user_name, team_name, roster_name)
    user.add_to_roster(user_name_2, team_name, roster_name)
    user.add_to_team(user_name, team_name_2)

    # Populate for team 1
    re = requests.post(api_v0('schedules/%s/populate' % schedule_id), json = {'start': time.time()})
    assert re.status_code == 200

    # Create conflicting primary event in team 2 for user 1
    re = requests.get(api_v0('events?team=%s' % team_name))
    assert re.status_code == 200
    events = re.json()
    assert len(events) == 2
    assert events[0]['user'] != events[1]['user']
    for e in events:
        event.create({
            'start': e['start'],
            'end': e['end'],
            'user': user_name,
            'team': team_name_2,
            'role': "primary",
        })

    # Populate again for team 1
    re = requests.post(api_v0('schedules/%s/populate' % schedule_id), json = {'start': time.time()})
    assert re.status_code == 200

    # Ensure events are both for user 2 (since user 1 is busy in team 2)
    re = requests.get(api_v0('events?team=%s&include_subscribed=false' % team_name))
    assert re.status_code == 200
    events = re.json()
    assert len(events) == 2
    assert events[0]['user'] == events[1]['user'] == user_name_2


@prefix('test_v0_populate_over')
def test_api_v0_populate_over(user, team, roster, role, schedule):
    user_name = user.create()
    team_name = team.create()
    role_name = role.create()
    roster_name = roster.create(team_name)
    schedule_id = schedule.create(team_name,
                                  roster_name,
                                  {'role': role_name,
                                   'events': [{'start': 0, 'duration': 604800}],
                                   'advanced_mode': 0,
                                   'auto_populate_threshold': 14})
    user.add_to_roster(user_name, team_name, roster_name)

    def clean_up():
        re = requests.get(api_v0('events?team='+team_name))
        for ev in re.json():
            requests.delete(api_v0('events/%d' % ev['id']))

    clean_up()

    now = time.time()
    re = requests.post(api_v0('schedules/%s/populate' % schedule_id), json = {'start': now})
    assert re.status_code == 200

    re = requests.get(api_v0('events?team=%s' % team_name))
    assert re.status_code == 200
    events = re.json()
    assert len(events) == 2
    # Weekly 12-hour schedule
    new_events = [{'start': 0, 'duration': 43200},
                  {'start': 86400, 'duration': 43200},
                  {'start': 172800, 'duration': 43200},
                  {'start': 259200, 'duration': 43200},
                  {'start': 345600, 'duration': 43200},
                  {'start': 432000, 'duration': 43200},
                  {'start': 518400, 'duration': 43200}]
    re = requests.put(api_v0('schedules/%s' % schedule_id), json={'events': new_events})
    assert re.status_code == 200

    re = requests.post(api_v0('schedules/%s/populate' % schedule_id), json = {'start': now})
    assert re.status_code == 200

    re = requests.get(api_v0('events?team=%s' % team_name))
    assert re.status_code == 200
    events = re.json()

    assert len(events) == 14
    clean_up()


@prefix('test_v0_populate_invalid')
def test_api_v0_populate_invalid(user, team, roster, role, schedule):
    user_name = user.create()
    team_name = team.create()
    role_name = role.create()
    roster_name = roster.create(team_name)
    schedule_id = schedule.create(team_name,
                                  roster_name,
                                  {'role': role_name,
                                   'events': [{'start': 0, 'duration': 604800}],
                                   'advanced_mode': 0,
                                   'auto_populate_threshold': 14})
    user.add_to_roster(user_name, team_name, roster_name)


    re = requests.post(api_v0('schedules/%s/populate' % schedule_id), json={'start': time.time() - 14 * 24 * 3600})
    assert re.status_code == 400

    # Check this is a no-op
    re = requests.get(api_v0('schedules/%s' % schedule_id))
    assert re.status_code == 200
    schedule_json = re.json()
    re = requests.post(api_v0('schedules/%s/populate' % schedule_id), json={'start': time.time() + 21 * 3600 * 24})
    assert re.status_code == 200
    re = requests.get(api_v0('schedules/%s' % schedule_id))
    assert re.status_code == 200
    re = requests.get(api_v0('schedules/%s' % schedule_id))
    assert re.json() == schedule_json


@prefix('test_v0_round_robin')
def test_api_v0_round_robin(user, team, roster, role, schedule, event):
    user_name = user.create()
    user_name_2 = user.create()
    user_name_3 = user.create()
    team_name = team.create()
    role_name = role.create()
    roster_name = roster.create(team_name)
    user.add_to_roster(user_name, team_name, roster_name)
    user.add_to_roster(user_name_2, team_name, roster_name)
    user.add_to_roster(user_name_3, team_name, roster_name)
    print("\n\n\nQQQQ ", team_name, roster_name, role_name, user_name, user_name_2, user_name_3, "\n\n\n")
    schedule_id = schedule.create(team_name,
                                  roster_name,
                                  {'role': role_name,
                                   'events': [{'start': 0, 'duration': 604800}],
                                   'advanced_mode': 0,
                                   'auto_populate_threshold': 28,
                                   'scheduler': {'name': 'round-robin',
                                                 'data': [user_name, user_name_2, user_name_3]}})

    def clean_up():
        re = requests.get(api_v0('events?team='+team_name))
        for ev in re.json():
            requests.delete(api_v0('events/%d' % ev['id']))

    clean_up()

    start = time.time()
    # Create an event for user 1
    event.create({'start': start,
                  'end': start + 1000,
                  'user': user_name,
                  'team': team_name,
                  'role': role_name})

    re = requests.post(api_v0('schedules/%s/populate' % schedule_id), json={'start': start + 2000})
    print(f"Status Code: {re.status_code}")
    print("Response Content:")
    print(re.text)
    assert re.status_code == 200

    re = requests.get(api_v0('events?team=%s' % team_name))
    assert re.status_code == 200
    events = re.json()
    # Check that newly populated events start with user 2, then loop back to user 1
    assert events[1]['user'] == user_name_2
    assert events[2]['user'] == user_name_3
    assert events[3]['user'] == user_name

    clean_up()


# Test skipping over matching events
@prefix('test_v0_populate_skip')
def test_api_v0_populate_skip(user, team, roster, role, schedule, event):
    user_name = user.create()
    user_name_2 = user.create()
    team_name = team.create()
    role_name = role.create()
    roster_name = roster.create(team_name)
    schedule_id = schedule.create(team_name,
                                  roster_name,
                                  {'role': role_name,
                                   'events': [{'start': 0, 'duration': 604800}],
                                   'advanced_mode': 0,
                                   'auto_populate_threshold': 14})
    user.add_to_roster(user_name, team_name, roster_name)
    user.add_to_roster(user_name_2, team_name, roster_name)

    re = requests.post(api_v0('schedules/%s/populate' % schedule_id), json = {'start': time.time()})
    assert re.status_code == 200

    re = requests.get(api_v0('events?team=%s' % team_name))
    assert re.status_code == 200
    events = re.json()
    assert len(events) == 2

    event.create({
        'start': events[0]['start'],
        'end': events[0]['end'],
        'user': user_name,
        'team': team_name,
        'role': role_name,
    })

    re = requests.post(api_v0('schedules/%s/populate' % schedule_id), json = {'start': time.time()})
    assert re.status_code == 200

    re = requests.get(api_v0('events?team=%s' % team_name))
    assert re.status_code == 200
    events = re.json()
    assert len(events) == 2

    schedule_ids = set([ev['schedule_id'] for ev in events])
    assert None in schedule_ids
    assert schedule_id in schedule_ids


# Test populate with subscription events in calendar
@prefix('test_v0_populate_subscription')
def test_api_v0_populate_subscription(user, team, roster, role, schedule, event):
    user_name = user.create()
    user_name_2 = user.create()
    team_name = team.create()
    team_name_2 = team.create()
    role_name = role.create()
    roster_name = roster.create(team_name)
    schedule_id = schedule.create(team_name,
                                  roster_name,
                                  {'role': role_name,
                                   'events': [{'start': 0, 'duration': 604800}],
                                   'advanced_mode': 0,
                                   'auto_populate_threshold': 14})
    user.add_to_roster(user_name, team_name, roster_name)
    user.add_to_roster(user_name_2, team_name, roster_name)
    user.add_to_team(user_name, team_name_2)
    re = requests.post(api_v0('teams/%s/subscriptions' % team_name), json={'role': role_name, 'subscription': team_name_2})
    assert re.status_code == 201

    # Populate for team 1
    re = requests.post(api_v0('schedules/%s/populate' % schedule_id), json = {'start': time.time()})
    assert re.status_code == 200

    # Create conflicting events in team 2 for user 1
    re = requests.get(api_v0('events?team=%s' % team_name))
    assert re.status_code == 200
    events = re.json()
    assert len(events) == 2
    assert events[0]['user'] != events[1]['user']
    for e in events:
        event.create({
            'start': e['start'],
            'end': e['end'],
            'user': user_name,
            'team': team_name_2,
            'role': role_name,
        })

    # Populate again for team 1
    re = requests.post(api_v0('schedules/%s/populate' % schedule_id), json = {'start': time.time()})
    assert re.status_code == 200

    # Ensure events are both for user 2 (since user 1 is busy in team 2)
    re = requests.get(api_v0('events?team=%s&include_subscribed=false' % team_name))
    assert re.status_code == 200
    events = re.json()
    assert len(events) == 2
    assert events[0]['user'] == events[1]['user'] == user_name_2