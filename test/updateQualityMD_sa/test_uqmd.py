#!/usr/bin/env python
#--------------------------------------------------------
# DREAM test harness/utility
#
#  Author:  Milan Novacek  (CVC)
#  Date:    Nov 20, 2013
#
# (c) 2013 Siemens Convergence Creators, s.r.o, Prague.
#--------------------------------------------------------

TEST_SUBJECT='IF-DREAM-O-UpdateQualityMD'

import json
import time

from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.encoders import encode_noop
from urllib2 import urlopen

METADATA = "short.xml"
DEBUG    = 0
SERVICE_URL = 'http://127.0.0.1:8000/ingest/uqmd/updateMD'

BLK_SZ = 8192

request = {
    "productID" : "prod1",
    "action"    : "add"
}

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

def encode_bin(msg):
    msg.add_header('Content-Transfer-Encoding', 'binary')

def create_mime_msg(request, data):
    outer = MIMEMultipart()
    msg1 = MIMEApplication(request, "json", encode_noop)
    outer.attach(msg1)
    if data:
        msg2 = MIMEApplication(data, "xml",  encode_bin)
        outer.attach(msg2)
    return outer

print "Testing Ingestion Engine 'add metadata' interface"
print "Date of test run: " + time.ctime()

fp = open(METADATA, "r")
metadata = fp.read()
fp.close()

n_errors = 0
print "    test valid add: ", 
mmsg = create_mime_msg(json.dumps(request), metadata).as_string()

if DEBUG > 0:
    print 'sending:'
    print mmsg

resp = json.loads(read_from_url(SERVICE_URL, mmsg))
if resp['status'] == 0:
    print "OK"
else:
    n_errors += 1
    print "FAILED"

print "    test error - missing metadata: ",
msg_no_data = create_mime_msg(json.dumps(request), None).as_string()
resp = json.loads(read_from_url(SERVICE_URL, msg_no_data))
if resp['status'] == 10: print "OK"
else:
    n_errors += 1
    print "FAILED"

print "    test error - bad request: ",
bad_request = {
    "productID" : "prod1",
    "action"    : "BadRequest"
}

mmsg = create_mime_msg(json.dumps(bad_request), metadata).as_string()
resp = json.loads(read_from_url(SERVICE_URL, mmsg))

if resp['status'] == 1:
    print "OK"
    if DEBUG>0:
        print "response:\n"+`resp`
else:
    n_errors += 1
    print "FAILED"

if not n_errors:
    print TEST_SUBJECT+": TESTS PASSED"

else:
    print "FAILED with "+`n_errors`+" failures."

