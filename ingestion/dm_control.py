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
import subprocess
import urllib2
import os, os.path, time
from collections import deque

import models
from utils import find_process_ids, pid_is_valid, setup_dm_paths
from settings import DOWNLOAD_MANAGER_DIR, DM_START_COMMAND, \
    IE_DOWNLOAD_DIR, DM_CONF_FN, BASH_EXEC_PATH

# The %s will be replaced by the port where DM is listening
DM_URL_TEMPLATE = "http://127.0.0.1:%s/download-manager/"
DM_DOWNDLOAD_COMMAND = "download"
IE_DAR_RESP_URL_TEMPLATE = "http://127.0.0.1:%s/ingest/darResponse"

MAX_PORT_WAIT_SECS     = 40
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
        self._dar_resp_url = None
        self._dar_queue = deque()

    def wait_for_port(self):
        if None == self._dm_port:
            self._logger.warning("No port to wait on!")
            return
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
                            msg = "Port OK, waited %2.1f secs." % waited
                            self._logger.info(msg)
                            found = True
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

    def start_dm(self):
        if not os.access(IE_DOWNLOAD_DIR, os.R_OK|os.W_OK):
            self._logger.info("Cannot write/read "+IE_DOWNLOAD_DIR+
                        ", attempting to create.")
            try:
                os.mkdir(IE_DOWNLOAD_DIR,0740)
                self._logger.info("Created "+IE_DOWNLOAD_DIR)
            except OSError as e:
                self._logger.error("Failed to create "+IE_DOWNLOAD_DIR+": "+`e`)

        if not os.access(DM_CONF_FN, os.R_OK|os.W_OK):
            self._logger.warning("Cannot access download mgr configuration "+
                           "("+DM_CONF_FN +"), ")
        else:
            try:
                self._dm_port = setup_dm_paths(
                    DM_CONF_FN, IE_DOWNLOAD_DIR, self._logger)
            except Exception as e:
                logger.error(
                    "Error setting/checking DM configuration: " + `e`)

        dm_config = models.Dm_config.objects.get(row_id=1)
        dm_pid = dm_config.dm_pid
        if not pid_is_valid(dm_pid):
            self._logger.warning(
                "Pid %s is not valid, searching.." % dm_pid)
            dm_pid = 0
        if dm_pid == 0:
            # check to see if it's not running anyway
            dm_pids = find_process_ids(
                ("java",
                "-DDM_HOME="+DOWNLOAD_MANAGER_DIR,
                "ngEO-download-manager",
                "download-manager-webapp-jetty-console.war")
                )
            if len(dm_pids) > 0:
                dm_pid = dm_pids[0]
                self._logger.info(
                    "Found %d running process(es)." % len(dm_pids))
                if len(dm_pids) > 1:
                    self._logger.warning(
                        "Too many running DMs?: " + `dm_pids`)
            else:
                self._logger.info("No running DM process found.")
        if dm_pid != 0:
            self._logger.info("DM is already running as pid "+`dm_pid`)
            dm_config.dm_pid = dm_pid
            dm_config.save()
        else:
            start_cmd = os.path.join(DOWNLOAD_MANAGER_DIR, DM_START_COMMAND)
            self._logger.info("Starting Download Manager:\n" + start_cmd)
            try:
                dm_process = subprocess.Popen(
                    (start_cmd,),
                    bufsize=-1,
                    executable=BASH_EXEC_PATH,
                    close_fds=True,
                    shell=True,
                    cwd=DOWNLOAD_MANAGER_DIR
                    )
            except Exception as e:
                self._logger.error(
                    "Failed to start the  Download Manager:\n" + `e`)
                return False

            dm_pid = dm_process.pid
            dm_config.dm_pid = dm_pid
            dm_config.save()
            self._logger.info("Download Manager started, pid="+`dm_pid`)
            # wait for DM to start listening on its port.
            self.wait_for_port()
        self._dm_url = DM_URL_TEMPLATE % self._dm_port
        return dm_pid != 0

    def stop_dm(self):
        # TODO
        print "TBD / not implemented: stop DM here"

    def submit_dar(self,dar):
        if None == self._dm_port:
            raise DMError("No port for DM")
        if None == self._ie_port:
            raise DMError("No IE port.")
        if None == self._dar_resp_url:
            self._dar_resp_url = IE_DAR_RESP_URL_TEMPLATE % self._ie_port

        self._dar_queue.append(dar)
        dm_dl_url = self._dm_url + DM_DOWNDLOAD_COMMAND
        post_data = "darUrl: "+self._dar_resp_url
TODO = """
  Put this into a utils function:
        resp = urllib2.urlopen(dm_dl_url, post_data )
        resp_str = 
            while True:
                buffer = r.read(blk_sz)
                if not buffer:
                    break
        if None != resp: resp.close()
"""
TODO = """
Assuming that this DAR has not been previously added, the following response will be returned:
{
   "success":true,
   "errorMessage":""
}

If the DAR has been previously added, a response similar to the following will be returned:

{
   "success":false,
   "errorMessage":"Error whilst adding DAR: DAR with url http://localhost:8080/download-manager-mock-web-server/static/manualDAR already exists",
   "errorType":"DataAccessRequestAlreadyExistsException"
}
"""
