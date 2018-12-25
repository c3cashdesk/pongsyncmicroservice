import sys

import datetime
import json
import os
import re
from urllib.parse import urljoin, urlencode
from urllib.request import urlopen, Request


def parse_isotime(timestamp):
    conformed_timestamp = re.sub(r"[:]|([-](?!((\d{2}[:]\d{2})|(\d{4}))$))", '', timestamp).replace('Z', '+0000')
    if '.' in conformed_timestamp:
        dt = datetime.datetime.strptime(conformed_timestamp, "%Y%m%dT%H%M%S.%f%z")
    else:
        dt = datetime.datetime.strptime(conformed_timestamp, "%Y%m%dT%H%M%S%z")

    dt = dt.astimezone(tz=datetime.timezone(datetime.timedelta(hours=1)))  # FIXME: Hardcoded CET
    return dt


postix_host = os.getenv('POSTIX_HOST')
if not postix_host:
    print("Please set environment variable POSTIX_HOST")
    sys.exit(1)

queue_sync_url = os.getenv('QUEUE_SYNC_URL')
if not queue_sync_url:
    print("Please set environment variable QUEUE_SYNC_URL")
    sys.exit(1)

queue_sync_token = os.getenv('QUEUE_SYNC_TOKEN')
if not queue_sync_token:
    print("Please set environment variable QUEUE_SYNC_TOKEN")
    sys.exit(1)

data = []

next_url = "/api/pings/?ponged=true&synced=false"
while next_url:
    r = Request(
        urljoin(postix_host, next_url),
    )
    with urlopen(r) as resp:
        if resp.status != 200:
            print("Invalid status code {}".format(resp.status_code))
            print(resp.read())
            sys.exit(2)
        resp_data = json.load(resp)
        next_url = resp_data['next']
        for r in resp_data['results']:
            data.append((r['id'], parse_isotime(r['pinged']), parse_isotime(r['ponged'])))

fmt = '%Y-%m-%d %H:%M:%S'
for pingid, pinged, ponged in data:
    r = Request(
        urljoin(queue_sync_url, '/pong'),
        data=urlencode({'ping': pinged.strftime(fmt), 'pong': ponged.strftime(fmt)}).encode(),
        headers={'Authorization': queue_sync_token}
    )
    with urlopen(r) as resp:
        if resp.status != 201:
            print("Invalid status code {}".format(resp.status))
            print(resp.read())
            sys.exit(2)

    r = Request(
        urljoin(postix_host, '/api/pings/{}/mark_synced/'.format(pingid)),
        method='POST'
    )
    with urlopen(r) as resp:
        if resp.status != 200:
            print("Invalid status code {}".format(resp.status))
            print(resp.read())
            sys.exit(2)
