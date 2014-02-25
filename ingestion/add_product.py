############################################################
#  Project: DREAM
#
#  Module:  Task 5 ODA Ingestion Engine 
#
#  Authors:
#           Vojtech Stefka(CVC),
#           Milan Novacek (CVC),
#
#  Date:   Sept 10, 2013
#
#    (c) 2013 Siemens Convergence Creators s.r.o., Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
#  Ingestion Engine: Add Product
#
#  Implements the interface  IF-DREAM-O-AddProduct.
#
############################################################

import logging
import os
import os.path
import shutil
import traceback
import datetime
import json
import sys
import subprocess

from settings import \
    IE_SCRIPTS_DIR, \
    IE_DEFAULT_ADDPROD_SCRIPT, \
    ADDPRODUCT_SUBDIR

from ingestion_logic import create_dl_dir
from utils import get_base_fname, mkFname
import work_flow_manager
import models

logger = logging.getLogger('dream.file_logger')

#
# Exception for local use within this file.
# To raise an error outside, use one of the exception classes
#  defined in utils.py
#
class AddProductError(Exception):
    msg = None

    def __init__(self, str):
        self.msg=str


def get_data_item(src_d, key):
    if not key in src_d:
        raise AddProductError("Missing '" + key + "' spec.")
    return src_d[key]

def parse_response_file(db_ref, fname):
    #
    # Expected content of the file:
    #  productId=<eo-id>
    #  url=<product-url>
    # At least one must be present; it is considered an error if neither
    # one is there (empty file or no file).
    #
    if not os.path.isfile(fname):
        raise AddProductError("Response file not a file or nonexistent.")

    try:
        fp = open(fname)
    except IOError:
        raise AddProductError("Cannot read response file")

    resp={}
    for line in fp:
        kv = line.split("=")
        resp[kv[0].strip()] =  kv[1].strip().replace('"','')
    
    fp.close()
    try:
        os.unlink(fname)
    except OSError as e:
        logger.warning("cannot remove "+fname+", "+`e`)

    if not resp or \
            ("productId" not in resp and "url" not in resp):
        raise AddProductError("No data in response file")

    if "productId" in resp:
        db_ref.new_product_id = resp["productId"]
    if "url" in resp:
        db_ref.product_url = resp["url"]

    return resp["productId"]


def add_product_wfunc(parameters):
    #
    # Executed by a worker thread from the the work_flow_manager 
    # Parameters - as set up in 'add_product' below:
    #       "task_type",
    #       "addProduct_id",
    #       "metadata",
    #       "product",
    #       "covId"
    #

    try:
        addProduct = models.ProductInfo.objects.filter(
            id=parameters["addProduct_id"])[0]
        addProduct.info_status = "processing"

        metadata = parameters["metadata"]
        product  = parameters["product"]

        if not os.path.isfile(product):
            logger.error("Product data Not found: "+product)
            raise AddProductError("Product not found or is not a file.")
        if not os.path.isfile(metadata):
            logger.error("Metadata Not found: "+metadata)
            raise AddProductError("Metadata not found or is not a file.")
        
        # move data and metadata to a permanent directory
        dl_dir, rp = create_dl_dir("ap_", ADDPRODUCT_SUBDIR)
        shutil.move(metadata, dl_dir)
        shutil.move(product, dl_dir)

        # Set up parameters for the script
        # The shell script is invoked by the IE with the following params:
        #   <action>
        #   -response=<filename>
        #   data
        #   metadata
        #
        # where <action> is one of:
        #  -replace=<existing_prod_id>
        #  -add         
        # Note '-add' is used if no existing product id is known, e.g.
        #  when addProduct is invoked from the RCOMS system.
        #
        # The response filename is where the script writes the new prodID,
        #  assuming all goes well.
        #

        meta_base = get_base_fname(metadata)
        data_base = get_base_fname(product)

        script = os.path.join(IE_SCRIPTS_DIR, IE_DEFAULT_ADDPROD_SCRIPT)

        resp_fname      = mkFname("addProdResp_")
        resp_full_fname = os.path.join(dl_dir,resp_fname)

        action = None
        if "covId" in parameters:
            action="-replace="+parameters["covId"]
        else:
            action="-add"

        process_status = subprocess.call(
            [script,
             action,
             "-dldir="+dl_dir,
             "-response="+resp_fname,
             "-meta="+meta_base,
             "-data="+data_base])

        if 0 != process_status:
            error_str ='AddProduct script returned status:'+`process_status`
            addProduct.info_error = error_str
            addProduct.info_status = "failed"
            logger.error("add_product_wfunc: " + error_str)
        else:
            cid = parse_response_file(addProduct, resp_full_fname)
            logger.info(
                "add_product_wfunc: script executed successfully, "+\
                    "new productId="+cid)
            addProduct.info_status = "success"

    except AddProductError as ap_err:
        logger.error("AddProductError in add_product_wfunc: " + ap_err.msg)
        addProduct.info_status = "failed"
        addProduct.info_error = ap_err.msg

    except Exception as e:
        logger.error("add_product_wfunc: Exception: " + `e`)
        addProduct.info_error = "Error %s" % `e`
        addProduct.info_status = "failed"
        from settings import IE_DEBUG
        if IE_DEBUG>0:
            traceback.print_exc(4,sys.stdout)

    finally:
            addProduct.save()

def add_product_submit(postData):
    logger.info("processing addProduct request")

    args = json.loads(postData)
    resp      = {}
    error_str = None
    status    = 0

    try:
        ap = models.ProductInfo(
            info_date      = datetime.datetime.utcnow(),
            info_status    = "processing",
            info_error     = "",
            new_product_id = "",
            product_url    = ""
            )
        ap.save()

        # Set up to pass input data to the worker task
        params = {
            "task_type"       : "ADD_PRODUCT",
            "addProduct_id"   : ap.id,

             # pathname of the product data for the product
            "product"        : get_data_item(args,"data")
            }

        # pathname of the metadata for the product
        if "metadata" in args:
            params["metadata"] = args["metadata"]

        # The coverage ID of an existing product, to be replaced
        if "productID" in args:
            params["covId"] = args["productID"]

        # for time-zone aware dates use this one instead:
        #   ap.info_date = datetime.datetime.utcnow().replace(tzinfo=utc)

        # process addProduct request by work_flow_manager
        wfm = work_flow_manager.WorkFlowManager.Instance()
        current_task = work_flow_manager.WorkerTask(params)
        wfm.put_task_to_queue(current_task)  # exectues add_product_wfunc

        resp['opId'] = ap.id
        resp['status'] = 0

    except AddProductError as ap_err:
        logger.error("AddProductError in add_product: " + ap_err.msg)
        status = 101
        error_str = ap_err.msg
        
    except OSError as e:
        logger.error("OSError in add_product: " + `e`)
        status = 102
        error_str = ("Internal Error: " + `e`)
        
    except Exception as e:
        status = 50
        error_str = "Unexpected exception: " + e.__class__.__name__
        logger.error("Exception in add_product: " + `e`)
        from settings import IE_DEBUG
        if IE_DEBUG>0:
            traceback.print_exc(4,sys.stdout)

    resp["status"] = status
    if error_str:
        resp["errorString"] = error_str
        logger.warning("addProduct request error. Status: " +`status`+\
                    ", error: " + error_str)
    else:
        if resp['opId']:
            logger.info("addProduct request status " +`status` +\
                        ", opId="+`resp['opId']`)
        else:
            logger.warning("addProduct: No opId nor error string, " +\
                               "status: "+`status`)
    return resp


