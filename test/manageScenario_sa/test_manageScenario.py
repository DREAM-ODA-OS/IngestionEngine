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

from urllib2 import \
    urlopen, \
    HTTPError

DEBUG    = 2
SERVICE_URL      = 'http://127.0.0.1:8000/ingest/ManageScenario/listScenarios'
GET_SCENARIO_URL = 'http://127.0.0.1:8000/ingest/ManageScenario/getScenario'
NEW_SCENARIO_URL = 'http://127.0.0.1:8000/ingest/ManageScenario/newScenario'

"""
NEW_SC_TEXT = {
        "productID" : "prod1_local",
        "metadata"  : meta_path,
        "data"      : data_path 
        }

{"scenario":{"scenario_name":"OpenSearch Scenario","dsrc":"http://some.url.com/dsrc","dsrc_type":"EO","view_angle":50.0,"to_date":"2011-01-10T00:00:00.000Z","from_date":"2010-01-10T00:00:00.000Z","aoi_type":"BB","aoi_bbox":{"lc":[48.397,42.59],"uc":[53.77,46.567]},"cloud_cover":50.0}}
"""

simple_check_list = [   "dsrc_type", "s2_preprocess", "download_subset", 
    "aoi_type",  "sensor_type",  "from_date", "to_date", "view_angle",
    "repeat_interval",  "cloud_cover",  "tar_result",  "dsrc", 
    "cat_registration",  "coastline_check"]

new_sc_input = {
    "dsrc_type": "EO", 
    "s2_preprocess": "NO", 
    "download_subset": 0, 
    "aoi_type": "BB", 
    "sensor_type": "TEST_1", 
    "from_date": "2000-04-08T01:00:00", 
    "to_date":   "2000-10-14T21:00:00", 
    "repeat_interval": 0, 
    "cloud_cover": 22.3, 
    "tar_result": 0, 
    "dsrc": "http://data.eox.at/instance00/ows", 
    "cat_registration": 0, 
    "coastline_check": 0, 
    "view_angle": 48.2, 
    "aoi_bbox": {
      "lc": [ 12.44,  49.1225 ], 
      "uc": [ 12.528, 49.2258 ]
    }, 
    "dssids": ["GISAT_SimS2_RGB_view", "TEST_DSSID"],
    "oda_server_ingest": 1, 
    "ncn_id": "new_sc_test", 
    "starting_date": "2013-11-11T11:22:33"
  }

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

#############################################
# Start test
#############################################
print "Testing Ingestion Engine interface "+TEST_SUBJECT
print "Date of test run: " + time.ctime()

n_errors = 0

#############################################
# listScenarios 
#############################################
print "    Issuing listScenarios request - ",
resp = json.loads(read_from_url(SERVICE_URL))
if DEBUG:
    print "\nresponse:\n    "+`resp`
if resp['status'] == 0 and 'scenarios' in resp:
    print "OK"
else:
    n_errors += 1
    print "FAILED"

#############################################
# getScenario
#############################################
scenarios = resp['scenarios']
sc0 = scenarios[0]
ncn_id = sc0['ncn_id']
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

#############################################
# newScenario
#############################################
print "    Issuing newScenario request - ",
newSc_url = NEW_SCENARIO_URL+'/'
try:
    newSc_resp =  json.loads(read_from_url(newSc_url, json.dumps(new_sc_input)))
except HTTPError as e:
    print "\n *** HTTPError: " + `e.code`
    print e.read()

print "    response:",
if DEBUG:
    print "\n"+json.dumps(newSc_resp, indent=2)

if newSc_resp['status'] == 0 and 'ncn_id' in newSc_resp:
    print "--> status OK"

ncn_id =  newSc_resp['ncn_id']
get_sc0_url = GET_SCENARIO_URL+'/id='+ncn_id

resp2 =  json.loads(read_from_url(get_sc0_url))
if DEBUG:
    print "\n"+json.dumps(resp2, indent=2)
if resp2['status'] == 0 and 'scenario' in resp2:
    print "--> status OK"

# check scneario data
sc_nn = resp2['scenario']
n_nn_errs = 0
try:
    for i in simple_check_list:
        if sc_nn[i] != new_sc_input[i]:
            n_nn_errs += 1
            print "  ** NOT matching:" +i+"    (get)  -!-  (reference)\n"
            print "           " + sc_nn[i]+" -!- "+new_sc_input[i]
except Exception as e:
    n_errors += 1
    print "ERROR: " +`e`

if n_nn_errs != 0: n_errors += 1

#############################################
# Report resutls
#############################################
print
if not n_errors:
    print TEST_SUBJECT+": TESTS PASSED"

else:
    print "FAILED with "+`n_errors`+" failures."

