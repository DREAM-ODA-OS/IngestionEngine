#!/usr/bin/env python
#--------------------------------------------------------
# DREAM test harness/utility
#
#  Author:  Milan Novacek  (CVC)
#  Date:    Nov 25, 2013
#
# (c) 2013 Siemens Convergence Creators, s.r.o, Prague.
#--------------------------------------------------------

TEST_SUBJECT='IF-DREAM-O-AddProduct'

"""
Operation URL: ingest/addProduct/addProduct

Request: HTTP POST

Input:
          JSON structure with the following fields:

-------   -----   --------------------------------------------------
metadata  string  The absolute pathname on the local filesystem of
                  the complete xml metadata for the product.
data      string  The absolute pathname on the local filesystem of
                  the coverage file for the new product.
productID string  The coverage ID of an existing product. The
                  existing product is to be replaced with the product
                  specified by 'data'.
---------------------------------------------------------------------
Returns:
          JSON structure with the following fields:

-------   -----   --------------------------------------------------
status    int     0 for no error.
opId      string  To be used for subsequent getStatus queries, if
                  status is 0.
errorString string May be provided in case the status is non-zero, to
                   report the cause of the failure. It is not guaranteed
                   that an error string is provided in all cases.
---------------------------------------------------------------------
"""

import json
import time
import os
import os.path
import shutil

from urllib2 import urlopen
from urllib2 import HTTPError

METADATA = "short.xml"
DATA     = "empty.tif"
DEBUG    = 0
SERVICE_URL = 'http://127.0.0.1:8000/ingest/addProduct/addProduct'
STATUS_URL  = 'http://127.0.0.1:8000/ingest/addProduct/getStatus'

# metadata and data are each copied to a tmp file.
# Each tmp file is moved to the download directory during the course of
# the addProduct operation.
TMP_META = "tttmeta.xml"
TMP_DATA = "tttdata.tif"

BLK_SZ = 8192

indent = ''

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

def check_status_ok(op_id):
    global indent
    print indent+"Chect status:"
    old_indent = indent
    indent += '    '
    try:
        st_url = STATUS_URL+"/id="+`op_id`
        print indent+"URL: "+st_url
        resp = json.loads(read_from_url(st_url))

        if not 'status' in resp:
            print indent+"No 'status' in getStatus response, resp:\n"+indent+`resp`
            return False

        if resp['status'] == 'processing':
            print indent+"AddProdcut status: "+`resp['status']`
            i = 0
            ts = time.time()
            while resp['status'] == 'processing':
                if i > 5:
                    tdiff = time.time() - ts
                    print indent+"Processing too long ("+`tdiff`+" secs)."
                    indent = old_indent
                    return False
                time.sleep(0.75)
                i += 1
                resp = json.loads(read_from_url(st_url))

        print indent+"AddProdcut status: "+`resp['status']`
                
        if resp['status'] != 'success':
            print indent+"Bad 'status' in getStatus response, resp:\n"+`resp`
            indent = old_indent
            return False

        else:
            indent = old_indent
            return True

    except Exception as e:
        print indent+"Unexpected Exception in check_status: " +`e`
        indent = old_indent
        return False


def prepare_data():

    if not os.path.exists(METADATA):
        print "FATAL: '" + METADATA+"' non-existent."
        exit(3)

    if not os.path.exists(DATA):
        print "Warning: creating empty '"+DATA+"'."
        fp = open(DATA, "w")
        fp.close()
        
    if os.path.exists(TMP_META):
        print TMP_META+" exists, exiting"
        exit(2)
    if os.path.exists(TMP_DATA):
        print TMP_DAtA+" exists, exiting"
        exit(2)

    shutil.copy( METADATA, TMP_META )
    shutil.copy( DATA,     TMP_DATA )

    cwd = os.getcwd()
    meta_path = os.path.join(cwd, TMP_META)
    data_path = os.path.join(cwd, TMP_DATA)
    
    return meta_path, data_path


def test_valid(meta_path, data_path):
    global indent
    print indent+"test valid addProduct: ",
    old_indent = indent
    indent += '    '
    errs = 0
    resp = None

    request = {
        "productID" : "prod1",
        "metadata"  : meta_path,
        "data"      : data_path 
        }

    if DEBUG > 0:
        print indent+'sending:'+json.dumps(request)

    try:
        resp = json.loads(read_from_url(SERVICE_URL, json.dumps(request)))
    except HTTPError as e:
        print
        print indent+"HTTPError: " + `e`
        resp['status'] = 99

    print "\n"+indent+"response from addProduct:\n"+indent+`resp`
    if resp['status'] == 0 and 'opId' in resp:
        if check_status_ok(resp['opId']):
            print indent+"OK"
        else:
            print indent+"Check_status failed unexpectedly"
            errs += 1
    else:
        print indent+"Bad/unexpected response:\n" + indent+`resp`
        errs += 1

    indent = old_indent
    if errs:
        print "\n"+indent+"TEST FAILED"
    return errs

def main():
    global indent
    n_errors = 0

    print "Testing Ingestion Engine 'addProduct' interface"
    print "Date of test run: " + time.ctime()

    meta_path, data_path = prepare_data()

    indent = '    '
    n_errors += test_valid(meta_path, data_path)

    indent = ''

    time.sleep(1)
    if os.path.exists(TMP_META): os.unlink(TMP_META)
    if os.path.exists(TMP_DATA): os.unlink(TMP_DATA)

    if not n_errors:
        print TEST_SUBJECT+": TESTS PASSED"
    else:
        print "FAILED with "+`n_errors`+" failures."



"""
print "    test error - missing metadata: ",
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

if resp['status'] == 1: print "OK"
else:
    n_errors += 1
    print "FAILED"
"""

if __name__ == '__main__':
    main()
