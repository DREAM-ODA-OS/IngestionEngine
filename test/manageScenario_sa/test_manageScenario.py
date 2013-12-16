#!/usr/bin/env python
#--------------------------------------------------------
# DREAM test harness/utility
#
#  Author:  Milan Novacek  (CVC)
#  Date:    Nov 20, 2013
#
# (c) 2013 Siemens Convergence Creators, s.r.o, Prague.
#--------------------------------------------------------

TEST_SUBJECT='IF-DREAM-O-ManageScenario'

import json
import time

from urllib2 import urlopen

METADATA = "short.xml"
DEBUG    = 0
SERVICE_URL      = 'http://127.0.0.1:8000/ingest/ManageScenario/listScenarios'
GET_SCENARIO_URL = 'http://127.0.0.1:8000/ingest/ManageScenario/getScenario'


BLK_SZ = 8192

def read_from_url(
    url,
    post_data=None,
    max_size=851200,    #bytes, 0 for unlimited
    read_timeout=300    #seconds, 0 for unlimited
    ):
    resp = None
    r = urlopen(url, post_data)
    end_time = time.time() + read_timeout
    while True:
        buff = r.read(BLK_SZ)
        if not buff:
            break
        if read_timeout > 0 and time.time() > end_time:
            raise IngestionError("URL read time expired")
        if None == resp: resp = buff
        else:            resp += buff
        if max_size > 0 and None != resp and len(resp) > max_size:
            raise IngestionError("Max read size exceeded")
    if None != r: r.close()
    return resp

print "Testing Ingestion Engine interface "+TEST_SUBJECT
print "Date of test run: " + time.ctime()

n_errors = 0

print "    Issuing listScenarios request",
resp = json.loads(read_from_url(SERVICE_URL))
if DEBUG:
    print "\nresponse:\n    "+`resp`
if resp['status'] == 0 and 'scenarios' in resp:
    print "OK"
else:
    n_errors += 1
    print "FAILED"

scenarios = resp['scenarios']
sc0 = scenarios[0]
ncn_id = sc0['id']
get_sc0_url = GET_SCENARIO_URL+'/id='+ncn_id

resp2 =  json.loads(read_from_url(get_sc0_url))

print "    scenario response:",
if DEBUG:
    print "\n"+json.dumps(resp2, indent=2)

if resp2['status'] == 0 and 'scenario' in resp2:
    print "OK"
else:
    n_errors += 1
    print "FAILED"

if not n_errors:
    print TEST_SUBJECT+": TESTS PASSED"

else:
    print "FAILED with "+`n_errors`+" failures."

