############################################################
#  Project: DREAM
#
#  Module:  Task 5 ODA Ingestion Engine 
#
#  Author: Milan Novacek (CVC)
#
#  Date:   Oct 29, 2013
#
#    (c) 2013 Siemens Convergence Creators s.r.o., Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
#  Ingestion Engine: Update Quality MetaData.
#
#  Implements the interface  IF-DREAM-O-UpdateQualityMD.
#
############################################################

import logging
import json
import time
import os.path
import subprocess
import traceback
from email import message_from_string

from settings import \
    IE_DEBUG, \
    IE_SCRIPTS_DIR, \
    IE_DEFAULT_UQMD_SCRIPT, \
    UQMD_SUBDIR

from ingestion_logic import create_dl_dir
from utils import mkFname, open_unique_file

MAX_NON_UNIQ_FILES = 50000

logger = logging.getLogger('dream.file_logger')

def write_md(prodId, metadata):
    try:
        dl_dir, rp = create_dl_dir(prodId+"_", UQMD_SUBDIR)
    except OSError as e:
        return 31, "Cannot create directory for storing MD", None

    leaf_root = mkFname(prodId+"_")
    meta_fp, meta_fname = open_unique_file(dl_dir, leaf_root, MAX_NON_UNIQ_FILES)
    meta_fp.write(metadata)
    meta_fp.close()

    return 0, '', meta_fname

def run_qmd_update(command, md_filename):
    action = command["action"]
    prodId = command["productID"]
    error_str = ''

    script = os.path.join(IE_SCRIPTS_DIR, IE_DEFAULT_UQMD_SCRIPT)
    
    status = subprocess.call(
        [script,
         "-"+action,
         prodId,
         md_filename])
    if 0 != status:
        error_str = 'Update Quality MetaData script returned status:'+`status`
    
    return status, error_str

def updateMetaData(postData):
    logger.info("processing updateMD request")

    resp = {}
    error_str = None
    status = 0
    metadata = None
    command = None
    productID = None
    action = None
    try:
        msg = message_from_string(postData)
        for part in msg.walk():
            # multipart/* are just containers
            if part.get_content_maintype() == 'multipart':
                continue
            elif part.get_content_maintype() == 'application':
                if part.is_multipart():
                    status = 24
                    error_str = "Unexpected Nested Multipart."
                    break
                subtype = part.get_content_subtype()
                if  'json' == subtype:
                    command = json.loads(part.get_payload())
                elif 'xml' == subtype:
                    metadata = part.get_payload()
                else:
                    status = 23
                    error_str = "Illegal Content Subtype: " + subtype
                    break
            else:
                status = 22
                error_str = \
                    "Illegal Content Main Type: " + \
                    part.get_content_maintype()
                break

        if None == metadata:
            status = 10
            error_str = "Missing metadata."
        if None == command:
            status = 11
            error_str = "Missing json part."
        elif not "productID" in command or not "action" in command:
            status = 12
            error_str = "Mandatory productID or action not found."

        if 0 == status:
            status, error_str, md_filename = write_md(
                command["productID"],metadata)
        if 0 == status:
            status, error_str = run_qmd_update(command, md_filename)

    except Exception as e:
        status = 50
        error_str = "Unexpected exception: " + e.__class__.__name__
        logger.error("Exception in updateMetaData: " + `e`)
        if IE_DEBUG>0:
            traceback.print_exc(4,sys.stdout)
            
        
    resp["status"] = status
    if error_str:
        resp["errorString"] = error_str
        logger.warning("updateMD request error. Status: " +`status`+\
                    ", error: " + error_str)
    else:
        logger.info("updateMD request finished with status " +`status`)
    return resp


