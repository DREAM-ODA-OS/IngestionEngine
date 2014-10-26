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
    ADDPRODUCT_SUBDIR, \
    IE_MAX_ADDPRODOPS, \
    IE_PURGE_ADDPRODOPS, \
    IE_ADDPRODOPS_AGE

from ingestion_logic import \
    create_dl_dir, \
    download_urls, \
    wait_for_download

from utils import \
    get_base_fname, \
    mkFname, \
    split_wcs_raw

import work_flow_manager
import models

logger = logging.getLogger('dream.file_logger')

# Max number of seconds to wait for download
IE_AP_MAX_DL_WAIT = 800

#
# Exception for local use within this file.
# To raise an error outside, use one of the exception classes
#  defined in utils.py
#
class AddProductError(Exception):
    msg = None

    def __init__(self, str):
        self.msg=str


def download_rem_product(dl_dir, rel_path, url):
    urls_with_dirs = [(rel_path, url)]
    logger.info("add_product: requesting download for\n"+`url`)
    dar_url, dar_id = download_urls(urls_with_dirs)
    if None == dar_id:
        raise AddProductError("No DAR generated")
    dl_errors = wait_for_download(None, dar_url, dar_id, None, IE_AP_MAX_DL_WAIT)
    if 0 != dl_errors:
        raise AddProductError("Download via DM failed.")

    files = os.listdir(dl_dir)
    len_files = len(files)
    if len_files == 0:
        raise AddProductError("Nothing downloaded.")
    if len_files > 1:
        logger.warning(
            "Found "+`len_files`+" in "+dl_dir+", expect 1.")

    f = files[0]
    logger.info("add_product: processing "+`f`)
    ret_str, metadata, product = split_wcs_raw(dl_dir, f, logger)
    if not ret_str:
        raise AddProductError("Failed to split file '"+f+"'.")

    return metadata, product


def prepare_local_product(dl_dir, metadata, product):
    # move data and metadata to a permanent directory

    # MP: Allow data as unpacked directories.
    if os.path.isfile(product) or os.path.isdir(product):
        try:
            logger.info("moving: %s --> %s"%(product, dl_dir))
            shutil.move(product, dl_dir)
        except OSError: #MP: If it cannot be moved try to copy them at least.
            logger.warning("Failed to move the data to the permanent location!")
            logger.info("copying: %s --> %s"%(product, dl_dir))
            if os.path.isdir(product):
                shutil.copytree(product, dl_dir)
            else:
                shutil.copy(product, dl_dir)
    else:
        logger.error("Product data Not found: "+product)
        raise AddProductError("Product not found or is not a file.")

    # MP: Metadata are required to be a file.
    if metadata and os.path.isfile(metadata):
        try:
            logger.info("moving: %s --> %s"%(metadata, dl_dir))
            shutil.move(metadata, dl_dir)
        except OSError: #MP: If it cannot be moved try to copy them at least.
            logger.warning("Failed to move the metadata to the permanent location!")
            logger.info("copying: %s --> %s"%(metadata, dl_dir))
            shutil.copy(metadata, dl_dir)
    else:
        #MP: Be less strict and allow missing metadata.
        logger.warning("Metadata Not found: %s"%metadata)
        #raise AddProductError("Metadata not found or is not a file.")
        metadata = None
        
    return metadata, product


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

def purge_old_ops():
    # This function should remove old entries in the database
    # and ensure that the max id number is less than IE_PURGE_ADDPRODOPS
    logger.info("add_product: purging _old ops.")
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(IE_ADDPRODOPS_AGE)
    models.ProductInfo.objects.filter(info_date__lt=cutoff).delete()


def add_product_wfunc(parameters):
    #
    # Executed by a worker thread from the the work_flow_manager 
    # Parameters - as set up in 'add_product' below:
    #       "task_type",
    #       "addProduct_id",
    #       "metadata",
    #       "product",
    #       "url"
    #       "covId"
    #
    # One of product or url are required
    #

    try:

        addProduct = models.ProductInfo.objects.filter(
            id=parameters["addProduct_id"])[0]
        addProduct.info_status = "processing"

        dl_dir, rel_path = create_dl_dir("ap_", ADDPRODUCT_SUBDIR)
        metadata = None
        product = None
        if 'url' in parameters:
            metadata, product = download_rem_product(
                dl_dir, rel_path, parameters["url"])
        elif 'product' in parameters:
            metadata, product = prepare_local_product(
                dl_dir, parameters.get("metadata"), parameters["product"])
        else:
            raise AddProductError("Neither product nor url in input.")

        if not product or (not metadata and 'url' in parameters):
            raise AddProductError(
                "internal error: Could not determine base names.")

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

        script = os.path.join(IE_SCRIPTS_DIR, IE_DEFAULT_ADDPROD_SCRIPT)

        resp_fname      = mkFname("addProdResp_")
        resp_full_fname = os.path.join(dl_dir,resp_fname)

        command = [script]

        if "covId" in parameters:
            command.append("-replace="+parameters["covId"])
        else:
            command.append("-add")

        command.append("-dldir="+dl_dir)
        command.append("-response="+resp_fname)
        if metadata is not None:
            command.append("-meta="+get_base_fname(metadata))
        command.append("-data="+get_base_fname(product))

        process_status = subprocess.call(command)

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
            logger.debug(traceback.format_exc(4))

    finally:
            addProduct.save()

def add_product_submit(postData):
    logger.info("processing addProduct request")

    args = json.loads(postData)
    resp      = {}
    error_str = None
    status    = 0

    from settings import IE_DEBUG
    if IE_DEBUG > 1:
        logger.debug("addProduct data:  " + `args`)

    try:
        ap = models.ProductInfo(
            info_date      = datetime.datetime.utcnow(),
            info_status    = "processing",
            info_error     = "",
            new_product_id = "",
            product_url    = ""
            )
        ap.save()

        if not 'data' in args and not 'url' in args:
            raise AddProductError(
                "Missing url or data; at least one is required")
            
        # Set up to pass input data to the worker task
        params = {
            "task_type"       : "ADD_PRODUCT",
            "addProduct_id"   : ap.id,

            }

        if 'data' in args and 'url' in args:
             logger.warning(
                 "add_product: only one of 'data' or 'url' should be used")

        # pathname of the product data for the product
        if 'data' in args:
            req_data = args["data"]
            if not req_data or None == req_data or len(req_data) < 1:
                logger.warning("add_product: empty 'data' in input")
            else:
                params['product'] = req_data.encode('ascii','ignore')


        # or url if any
        if 'url' in args:
            req_url = args["url"]
            if not req_url or req_url == None or len(req_url) < 2:
                logger.warning("add_product: empty 'url' in input")
            else:
                params["url"] = req_url.encode('ascii','ignore')

        # pathname of the metadata for the product
        if "metadata" in args:
            params["metadata"] = args["metadata"].encode('ascii','ignore')

        # The coverage ID of an existing product, to be replaced
        if "productID" in args:
            productID = args["productID"]
            if not productID or None == productID or len(productID) < 1:
                logger.warning("add_product: empty 'productID'")
            else:
                params["covId"] = productID.encode('ascii','ignore')

            
        # for time-zone aware dates use this one instead:
        #   ap.info_date = datetime.datetime.utcnow().replace(tzinfo=utc)

        # process addProduct request by work_flow_manager
        wfm = work_flow_manager.WorkFlowManager.Instance()
        current_task = work_flow_manager.WorkerTask(params)
        wfm.put_task_to_queue(current_task)  # exectues add_product_wfunc

        resp['opId'] = ap.id
        resp['status'] = 0

        if ap.id > IE_MAX_ADDPRODOPS:
            error_str = ("Internal Error in add_product: " +
                         "ap.id exceeded max (" +
                         `IE_MAX_ADDPRODOPS` + ")")
            status = 103
            logger.error(error_str)
            
        if ap.id > IE_PURGE_ADDPRODOPS:
            purge_old_ops()

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

