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
import os, os.path, time
from collections import deque

from utils import find_process_ids, pid_is_valid, get_dm_config, \
    read_from_url, DMError
from settings import DM_CONF_FN, MAX_PORT_WAIT_SECS, IE_DEBUG

# The %s will be replaced by the port where DM is listening
DM_URL_TEMPLATE = "http://127.0.0.1:%s/download-manager/"
DM_DOWNDLOAD_COMMAND  = "download"
DM_DAR_STATUS_COMMAND = "dataAccessRequests"
IE_DAR_RESP_URL_TEMPLATE = "http://127.0.0.1:%s/ingest/darResponse"

DEFAULT_PORT_WAIT_SECS = 20
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
                        "Wait time elapsed without finding open port.")
                    break
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
                logger.error(
                    "Error checking DM configuration: " + `e`)
                raise

        if None == self._download_dir or '' == self._download_dir:
            raise  DMError("No download directory")

        if None == self._dm_port or '' == self._dm_port:
            raise  DMError("No DM port")

        if not os.access(self._download_dir, os.R_OK|os.W_OK):
            self._logger.info("Cannot write/read "+self._download_dir+
                        ", attempting to create.")
            try:
                os.mkdir(self._download_dir,0740)
                self._logger.info("Created "+self._download_dir)
            except OSError as e:
                msg = "Failed to create "+self._download_dir+": "+`e`
                self._logger.error(msg)
                raise  DMError(msg)

        port_ok = self.wait_for_port()
        self._dm_url = DM_URL_TEMPLATE % self._dm_port
        return port_ok

    def submit_dar(self, dar):
        if None == self._dm_port:
            raise DMError("No port for DM")
        if None == self._ie_port:
            raise DMError("No IE port.")
        if None == self._dar_resp_url:
            self._dar_resp_url = IE_DAR_RESP_URL_TEMPLATE % self._ie_port

        self._dar_queue.append(dar)
        dm_dl_url = os.path.join(self._dm_url, DM_DOWNDLOAD_COMMAND)
        post_data = "darUrl="+self._dar_resp_url
        self._logger.info("Submitting request to DM to retieve DAR:\n" \
                           + post_data)
        dm_resp = json.loads(read_from_url(dm_dl_url, post_data))
        self._logger.debug("dm_response: " + `dm_resp`)
        dar_id = None
        if "success" in dm_resp:
            print "success in dm_resp +++++++++"
            zz = dm_resp["success"]
            print "zz:"+`zz`
        else:
            print " NOOO success in dm_resp --------"
        if "success" in dm_resp and dm_resp["success"]:
            self._logger.info("DM accepted DAR. TODO: set DAR id.")
            dar_id = "TODO"  #TODO: set dar_id if/when the DM supplies one.
            if IE_DEBUG > 0:
                print "dm_resp:\n" + `dm_resp`
        elif "errorType" in dm_resp and \
                dm_resp["errorType"] == "DataAccessRequestAlreadyExistsException":
            return ("DAR_EXISTS", None)
        else:
            msg = None
            if not "errorMessage" in dm_resp:
                msg = "Unknown error, no 'errorMessage' found in response"
            else:
                msg = "DM reports error:\n" + dm_resp["errorMessage"]
            raise DMError(msg)
        return ("OK", dar_id)

    def get_next_dar(self):
        if len(self._dar_queue) == 0:
            return None
        else:
            return self._dar_queue.popleft()

    def set_ie_port(self, port):
        self._ie_port = port

    def set_condIEport(self,newport):
        if None == self._ie_port:
            self._ie_port = newport

