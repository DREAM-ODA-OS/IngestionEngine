############################################################
#  Project: DREAM
#  Module:  Task 5 ODA Ingestion Engine 
#  Author: Milan Novacek   (CVC)
#  Date:   Nov 12, 2013
#
#    (c) 2013 Siemens Convergence Creators s.r.o., Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
# Ingestion engine - download manager inteface and control.
#
############################################################

from singleton_pattern import Singleton
import logging
import urllib2
import json
import os, os.path
import time
import sys
import threading
from collections import deque

from utils import find_process_ids, pid_is_valid, get_dm_config, \
    read_from_url, DMError, mkIdBase, check_or_make_dir
from settings import DM_CONF_FN, MAX_PORT_WAIT_SECS, IE_DEBUG

# The %s will be replaced by the port where DM is listening
DM_URL_TEMPLATE = "http://127.0.0.1:%s/download-manager/"
DM_DOWNDLOAD_COMMAND  = "download"
DM_DAR_STATUS_COMMAND = "dataAccessRequests"

# the string is the uuid
DM_PRODUCT_CANCEL_TEMPLATE = '/products/%s?action=cancel'

IE_DAR_RESP_URL_TEMPLATE = "http://127.0.0.1:%s/ingest/darResponse"

DEFAULT_PORT_WAIT_SECS = 25
SYSTEM_PROC_NET_PATH   = "/proc/net/tcp"
PROC_UID_INDEX         = 7
PROC_STATUS_INDEX      = 3
PROC_ADDRESS_INDEX     = 1


@Singleton
class DownloadManagerController:
    def __init__(self):
        self._logger = logging.getLogger('dream.file_logger')
        self._dm_port = None   # string
        self._dm_url  = None
        self._ie_port = None   # string, ingestion engine port
        self._download_dir = None
        self._dar_resp_url = None
        self._dar_queue = deque()
        self._lock_queue = threading.Lock()
        self._seq_id  = 0

    def get_download_dir(self):
        return self._download_dir

    def wait_for_port(self):
        port_found = False
        if None == self._dm_port:
            self._logger.warning("No port to wait on!")
            return port_found
        self._logger.info("Waiting for DM port "+self._dm_port)
        uid = "%d" % os.getuid()
        start_time = time.time()
        end_time = start_time + MAX_PORT_WAIT_SECS
        proc = None
        dm_port = int(self._dm_port)
        found = False
        try:
            while True:
                proc = open(SYSTEM_PROC_NET_PATH, "r")
                for l in proc:
                    fields = l.split()
                    if fields[PROC_UID_INDEX] == uid and \
                            fields[PROC_STATUS_INDEX] == '0A':
                        p = int(fields[PROC_ADDRESS_INDEX].split(":")[1],16)
                        if p == dm_port:
                            waited = time.time() - start_time
                            msg = "DM Port OK, waited %2.1f secs." % waited
                            self._logger.info(msg)
                            found = True
                            port_found = True
                            break
                proc.close()
                proc = None
                if found: break
                time.sleep(1)
                if time.time() > end_time:
                    self._logger.warning(
                        "Wait time elapsed without finding the listening port.")
                    break
                sys.stdout.write(".")
                sys.stdout.flush()
        except Exception as e:
            self._logger.warning(
                "Internal Error in wait_for_port() %s\n" % `e` +
                "       waiting %d seconds." % DEFAULT_PORT_WAIT_SECS)
            time.sleep(DEFAULT_PORT_WAIT_SECS)
            self._logger.info("Finished default wait.")
        finally:
            if None != proc: proc.close()
        return port_found

    def configure(self):
        if not os.access(DM_CONF_FN, os.R_OK):
            self._logger.warning("Cannot access download mgr configuration "+
                           "("+DM_CONF_FN +"), ")
        else:
            try:
                self._dm_port, self._download_dir = get_dm_config(
                    DM_CONF_FN,
                    self._logger)
            except Exception as e:
                self._logger.error(
                    "Error checking DM configuration: " + `e`)
                raise

        if None == self._download_dir or '' == self._download_dir:
            raise  DMError("No download directory")

        if None == self._dm_port or '' == self._dm_port:
            raise  DMError("No DM port")

        check_or_make_dir(self._download_dir, self._logger)
        #now add this year's dir
        year = str(time.gmtime().tm_year)
        yr_dld = os.path.join(self._download_dir,year)
        check_or_make_dir(yr_dld, self._logger)

        port_ok = self.wait_for_port()
        self._dm_url = DM_URL_TEMPLATE % self._dm_port
        return port_ok

    def _get_next_seq_id(self):
        if self._seq_id >= sys.maxint - 1:
            self._seq_id = 0
        else:
            self._seq_id += 1
        return self._seq_id

    def submit_dar(self, dar):
        if None == self._dm_port:
            raise DMError("No port for DM")
        if None == self._ie_port:
            raise DMError("No IE port.")
        if None == self._dar_resp_url:
            self._dar_resp_url = IE_DAR_RESP_URL_TEMPLATE % self._ie_port

        # lock the queue since access is potentially from the
        # work_flow_manager worker threads
        self._lock_queue.acquire()
        try:
            dar_seq_id = mkIdBase()+`self._get_next_seq_id()`
            dar_q_item = (dar_seq_id, dar)
            self._dar_queue.append(dar_q_item)
        except Exception as e:
            raise
        finally:
            self._lock_queue.release()

        dm_dl_url = os.path.join(self._dm_url, DM_DOWNDLOAD_COMMAND)
        dar_url = self._dar_resp_url+"/"+dar_seq_id
        post_data = "darUrl="+dar_url
        self._logger.info("Submitting request to DM to retieve DAR:\n" \
                           + post_data)
        dm_resp = json.loads(read_from_url(dm_dl_url, post_data))
        self._logger.debug("dm_response: " + `dm_resp`)
        dm_dar_id = None
        if "success" in dm_resp and dm_resp["success"]:
            self._logger.info("DM accepted DAR. TODO: set DAR id.")
            dm_dar_id = "TODO"  #TODO: set dm_dar_id if/when the DM supplies one.
        elif "errorType" in dm_resp and \
                dm_resp["errorType"] == "DataAccessRequestAlreadyExistsException":
            # TODO: try to auto-recover/find the status/resume the dar
            #       but this should occur only exceptionally/almost never
            return ("DAR_EXISTS", None, None)
        else:
            msg = None
            if not "errorMessage" in dm_resp:
                msg = "Unknown error, no 'errorMessage' found in response"
            else:
                msg = "DM reports error:\n" + dm_resp["errorMessage"]
            raise DMError(msg)
        
        return ("OK", dar_url, dm_dar_id)

    def get_next_dar(self, dar_seq_id):
        if len(self._dar_queue) == 0:
            return None
        else:
            if self._dar_queue[0][0] == dar_seq_id:
                dar = self._dar_queue.popleft()[1]
            else:
                self._logger.warning("Out-of-sequence dar access, dar_seq_id:" + \
                                         `dar_seq_id`)
                dar = self.find_dar(dar_seq_id)
            return dar
    
    def find_dar(dar_seq_id):
        for d in self._dar_queue:
            if d[0] == dar_seq_id:
                return d[1]
        return None

    def set_ie_port(self, port):
        self._ie_port = port

    def set_condIEport(self,newport):
        if None == self._ie_port:
            self._ie_port = newport

