############################################################
#  Project: DREAM
#  Module:  Task 5 ODA Ingestion Engine 
#  Author: Milan Novacek (CVC)
#  Date:   Sept 16, 2013
#
#    (c) 2013 Siemens Convergence Creators s.r.o., Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
#  Ingestion Engine: core ingestion logic.
#
############################################################

import logging
import os
import sys
import urllib2
import json
import time
import traceback
import simplejson

import dar_builder
import work_flow_manager

from utils import \
    Bbox, \
    TimePeriod, \
    build_aoi_toi, \
    mkFname, \
    DMError, \
    IngestionError, \
    StopRequest, \
    read_from_url, \
    check_or_make_dir, \
    make_new_dir

from settings import \
    IE_DEBUG, \
    DAR_STATUS_INTERVAL,\
    STOP_REQUEST, \
    IE_30KM_SHPFILE, \
    IE_DM_MAXWAIT2

from dm_control import \
    DownloadManagerController, \
    DM_DAR_STATUS_COMMAND, \
    DM_PRODUCT_CANCEL_TEMPLATE

from models import \
    Archive, \
    Scenario, \
    ScenarioStatus, \
    DSRC_EOWCS_CHOICE, \
    DSRC_OSCAT_CHOICE, \
    DSRC_BGMAP_CHOICE, \
    AOI_BBOX_CHOICE,   \
    AOI_POLY_CHOICE,   \
    AOI_SHPFILE_CHOICE, \
    date_from_iso8601

from coastline_ck import \
    coastline_ck, \
    coastline_cache_from_aoi

# XML metadata parsing
from ie_xml_parser import \
    parse_file, \
    extract_path_text, \
    extract_Id, \
    extract_gml_bbox, \
    extract_WGS84bbox, \
    extract_TimePeriod, \
    extract_om_time, \
    extract_ServiceTypeVersion, \
    extract_DatasetSeriesSummaries, \
    extract_CoverageId, \
    extract_prods_and_masks, \
    get_coverageDescriptions, \
    SENSOR_XPATH, \
    INCIDENCEANGLE_XPATH, \
    CLOUDCOVER_XPATH

# misc. constants
SERVICE_WCS        = "service=wcs"
WCS_GET_CAPS       = "request=GetCapabilities"
EOWCS_DESCRIBE_CS  = "request=DescribeEOCoverageSet"
WCS_GET_COVERAGE   = "request=GetCoverage"
WCS_IMAGE_FORMAT   = "format=image/tiff&mediatype=multipart/mixed"

CAPABILITIES_TAG   = "Capabilities"
EOCS_DESCRIPTION_TAG = "EOCoverageSetDescription"

# CRS
EPSG_4326 = 'http://www.opengis.net/def/crs/EPSG/0/4326'

# For debugging:
# MAX_DEOCS_URLS limits the number of DescribeEOCoverageSet requests issued,
# MAX_GETCOV_URLS limits the number of GetCoverage requests generated
# 0 means unlimited
DEBUG_MAX_DEOCS_URLS  = 0
DEBUG_MAX_GETCOV_URLS = 0


logger = logging.getLogger('dream.file_logger')


# ------------ utilities --------------------------

def stop_downloads():
    logger = logging.getLogger('dream.file_logger')
    logger.info("stop_downloads")
    

def get_dssids_from_pf(product_facility, aoi_toi):
    # gets dssids from the pf
    # returns service_version, dssids
    #  where service_version is a string and ddsids is an array (list).

    ids_from_pf = []

    caps = get_caps_from_pf(product_facility)

    if None == caps:
        logger.warning("No capabilities were obtained from: "+`product_facility`)
        return ids_from_pf

    try:
        service_version = extract_ServiceTypeVersion(caps).strip()
        wcseo_dss_list  = extract_DatasetSeriesSummaries(caps)
    except Exception as e:
        logger.error("Exception in get_dssids_from_pf: " + `e`)

    if IE_DEBUG > 0:
        logger.debug("get_dssids_from_pf: dss_list len="+`len(wcseo_dss_list)`)

    caps = None  # no longer needed

    ids_from_pf = getDssList(None, wcseo_dss_list, aoi_toi)

    if IE_DEBUG > 0:
        logger.debug("get_dssids_from_pf: num ids="+`len(ids_from_pf)`)

    if len(ids_from_pf) < 1:
        logger.warning("No DSS-IDs matching the criteria are available from the PF")

    return service_version, ids_from_pf


def check_status_stopping(scid):
    if None == scid: return False
    status = ScenarioStatus.objects.get(scenario_id=scid).status
    return status == STOP_REQUEST

def wfm_set_dar(scid, darid):
    work_flow_manager.WorkFlowManager.Instance().set_active_dar(scid, darid)

def wfm_clear_dar(scid):
    work_flow_manager.WorkFlowManager.Instance().set_active_dar(scid, '')

def file_is_zero(fname):
    return 0 == os.stat(fname)[stat.ST_SIZE]

def create_dl_dir(leaf_name_root, extradir=None):
    # structure of dirs created, shown by example:
    # 1) no extradir:
    #    2013
    #     +-10
    #        +-leaf_name_root05_120101_55abbbc
    #            ...
    #
    # 2) with extradir:
    #    2013
    #     +-10
    #        +-extradir
    #            +-leaf_name_root05_120102_44deeef
    #                ...

    root_dl_dir = DownloadManagerController.Instance().get_download_dir()
    st_time = time.gmtime()
    yr_name  = str(st_time.tm_year)
    mon_name = str(st_time.tm_mon)
    leaf_dir_name = mkFname(leaf_name_root)

    if extradir:
        path = (yr_name, mon_name, extradir, leaf_dir_name)
    else:
        path = (yr_name, mon_name, leaf_dir_name)
    
    rel_path  = os.path.join(*path)

    full_path = root_dl_dir
    for p in path[:-1]:
        full_path = os.path.join(full_path, p)
        check_or_make_dir(full_path, logger)

    full_path = os.path.join (full_path, leaf_dir_name)
    make_new_dir(full_path, logger)

    return full_path, rel_path


# ------------ status reporting  -------------------
def set_status(sc_id, st_text, percentage):
    if None == sc_id: return
    work_flow_manager.WorkFlowManager.Instance().set_scenario_status(
        0, sc_id, 0, st_text, percentage)


# ------------ processing --------------------------
def getXmlTree(url, expected_tag):
    err  = None
    resp = None
    try:
        resp = urllib2.urlopen( url )
    except urllib2.URLError as e:
        err = e
    except urllib2.HTTPError as e:
        err = e

    if None != err:
        err_str = "error accessing data source with url '"+url+"': "
        try:
            err_str = err_str + `err.code` + " "
        except:
            pass
        try:
            err_str = err_str + `err.reason`
        except:
            pass
        logger.error( "ERROR: " + err_str )
        if None!=resp: resp.close()
        return (None, None)
    else:
        return ( resp, parse_file(resp, expected_tag, resp.geturl()) )

def getDssList(scid, eo_dss_list, aoi_toi):
    # get list of datasets that overlap bbox and timeperiod
    id_list = []
    req_bb, req_time = aoi_toi
    for dss in eo_dss_list:
        if scid and check_status_stopping(scid):
            raise StopRequest("Stop Request")

        timeperiod = extract_TimePeriod(dss)
        if None == timeperiod:
            logger.warning("Failed to extract time range from " + `dss`)
            continue
        if not req_time.overlaps(timeperiod):
            continue

        bb1 = extract_WGS84bbox(dss)
        if None == bb1:
            logger.warning("Failed to extract bb from " + `dss`)
            continue
        if bb1.overlaps( req_bb ):
            id_list.append( extract_Id(dss) )

    return id_list

def should_check_coastline(params):
    if not 'coastline_check' in params:
        return True
    if params['coastline_check']:
        return True
    return False

def get_caps_from_pf(product_facility_url):
    base_url = product_facility_url + "?" + SERVICE_WCS
    url_GetCapabilities = base_url + "&" + WCS_GET_CAPS
    (fp, caps) = getXmlTree(url_GetCapabilities, CAPABILITIES_TAG)
    ret = None
    if None == caps:
        logger.error("Cannot parse getCap file. Url="+url_GetCapabilities)
    else:
        ret = caps

    if None != fp: fp.close()
    return ret

def check_bbox(coverageDescription, req_bbox):
    bb = extract_gml_bbox(coverageDescription)
    if None == bb:
        return False
    return bb.overlaps(req_bbox)

def check_coastline(coverageDescription, cid, params, ccache):
    if not should_check_coastline(params):
        return True
    return coastline_ck(coverageDescription, cid, ccache)

def check_timePeriod(coverageDescription, req_tp, md_src):
    if req_tp == None:     return True
    timePeriod = extract_om_time(coverageDescription)
    if None==timePeriod:
        logger.warning("timePeriod not found in EO metatada, src='"+\
                md_src+"'")
        return False
    return timePeriod.overlaps(req_tp)

def check_custom_conditions(cd, req):
    # implements AND between all custom conditions
    custom = None
    if 'custom' in req:
        custom = req['custom']
    
    if not custom:
        return True

    leaf_nodes = None
    for c in custom:
        if not c[0]: continue
        searchtext = ".//"+c[0]
        try:
            leaf_nodes = cd.findall(searchtext)
        except Exception as e:
            logger.error("Error in custom condition, cond:\n" + \
                             `c[0]` + ", error:\n" + `e`)
            return False

        if not leaf_nodes:
            return False

        if c[1]:
            found = False
            for l in leaf_nodes:
                if l.text == c[1]:
                    found =  True
                    break
            if not found:
                return False
        # else there is a node but no text to match, so
        # we consider it to be true, e.g. if attributes
        # match

    return True


def check_text_condition(cd, req, key, xpath):
    if not key in req:
        return True
    req_item = req[key]
    if req_item == '':
        return True
    md_item  = extract_path_text(cd, xpath)
    if not md_item:
        return True
    return req_item == md_item


def check_float_max(cd, req, key, xpath, use_abs=False):
    if not key in req:
        logger.warning("Check of " + `key` + ": not found in request")
        return True
    req_item = req[key]
    try:
        req_float = float(req_item)
    except Exception as e:
        raise IngestionError("Bad value specified for " + `key` + \
                                 ', exception: ' + `e`)
    md_item  = extract_path_text(cd, xpath)
    if not md_item:
        logger.warning("Check of " + `key` + ": not found in metadata.")
        return True

    try:
        md_float = float(md_item)
        if use_abs:
            md_float = abs(md_float)
    except Exception as e:
        logger.warning("unexpected error converting value from metadata"+\
                           "for "+`key`+"', exception: " + `e`)
        return True

    if md_float <= req_float:
        if IE_DEBUG>1:
            logger.info("Accepted " + key + " MD value " + `md_float`)
        return True


def gen_dl_urls(params, aoi_toi, base_url, md_url, eoid, ccache):
    """ params is the dictionary of input parameters
        aoi_toi is a tuple containing Area-of-interest Bounding-Box and
                the Time of Interest time range
        md_url  is the metadata url

        This function generates Download URLs:
           1. get_coverage requests based on metadata from the
              DescribeEOCoverageSet request - depending if the metadata
              matches the scenario params, and
           2. Other ULR refs found in the metadata the product and mask
              elements:
                //eop:product//eop:fileName/ows:ServiceReference[@xlink:href]
                //eop:mask//eop:fileName/ows:ServiceReference[@xlink:href]
    """

    if IE_DEBUG > 0:
        logger.info("Generating DL-URLS from DescribeEOCoverageSet: '"+md_url+"'")

    ret = []
    scid = params['sc_id']

    # cd_tree: coverage description tree extracted from the
    #          metadata XML file
    (fp, cd_tree) = getXmlTree(md_url, EOCS_DESCRIPTION_TAG)

    if check_status_stopping(scid):
        if None != fp: fp.close()
        raise StopRequest("Stop Request")

    if None==cd_tree:
        if None != fp: fp.close()
        return ret

    if IE_DEBUG > 1:
        try:
            nreturned = cd_tree.attrib['numberReturned']
            nmatched  = cd_tree.attrib['numberMatched']
            logger.info("    MD reports nreturned = "+`nreturned`+\
                            ", nmatched =  "+`nmatched`)
        except KeyError:
            pass
    
    cds = get_coverageDescriptions(cd_tree)
    if len(cds) < 1:
        logger.warning("No CoverageDescriptions found in '"+md_url+"'")

    should_check_archived = True
    if 'check_arch' in params:
        should_check_archived = params['check_arch']
    failed = set()
    passed = 0
    for cd in cds:

        if check_status_stopping(scid):
            if None != fp: fp.close()
            raise StopRequest("Stop Request")

        coverage_id = extract_CoverageId(cd)
        if None == coverage_id:
            logger.error("EOID " + `eoid` +
                         " Cannot find CoverageId in '"+md_url+"'")
            continue
        if IE_DEBUG > 2:
            logger.debug("  coverage_id="+coverage_id)
        
        if should_check_archived and check_archived(scid, coverage_id):
            if IE_DEBUG > 0:
                logger.debug("  coverage_id='"+coverage_id+
                            "' is achived, not downloading.")
            continue

        if not check_bbox(cd, aoi_toi[0]):
            if IE_DEBUG > 2: logger.debug("  bbox check failed.")
            if IE_DEBUG > 0: failed.add('bbox')
            continue
        
        if not check_timePeriod(cd, aoi_toi[1], md_url):
            if IE_DEBUG > 2: logger.debug("  TimePeriod check failed.")
            failed.add('TimePeriod')
            continue

        if not check_text_condition(cd, params, 'sensor_type', SENSOR_XPATH):
            if IE_DEBUG > 2: logger.debug("  sensor type check failed.")
            if IE_DEBUG > 0: failed.add('sensor_type')
            continue

        if not check_float_max(cd, params, 'view_angle', INCIDENCEANGLE_XPATH, True):
            if IE_DEBUG > 2: logger.debug("  incidence angle check failed.")
            if IE_DEBUG > 0: failed.add('view_angle')
            continue

        if not check_float_max(cd, params, 'cloud_cover', CLOUDCOVER_XPATH):
            if IE_DEBUG > 2: logger.debug("  cloud cover check failed.")
            if IE_DEBUG > 0: failed.add('cloud_cover')
            continue

        if not check_coastline(cd, coverage_id, params, ccache):
            if IE_DEBUG > 2: logger.debug("  coastline check failed.")
            if IE_DEBUG > 0: failed.add('coastline check')
            continue

        if not check_custom_conditions(cd, params):
            if IE_DEBUG > 2: logger.debug("  custom conds check failed.")
            if IE_DEBUG > 0: failed.add('custom conditions')
            continue

        passed = passed+1
        ret.append(base_url+"&CoverageId="+coverage_id)

        #  disabled, not supported by ODA server for now:
        #ret.extend( extract_prods_and_masks(cd, True) )


    if None != fp: fp.close()
    cd_tree = None
    if IE_DEBUG > 0:
        logger.info( "EOID " + `eoid` +
                     " cov descriptions passed: "+`passed`+" / "+`len(cds)`)
    if IE_DEBUG > 0 and IE_DEBUG < 3:
        logger.info( "EOID " + `eoid` +" summary of conditions failed: " +
                     `[f for f in failed]`)
        
    if IE_DEBUG > 1:
        dbg_gen_urls = "\n    ".join(ret)
        logger.info( "EOID " + `eoid` + " generated URLs:\n    " + dbg_gen_urls)
    del cd_tree
    return ret
    

def process_csDescriptions(params, aoi_toi, service_version, md_urls):
    """ Input: md_urls is a tuple, where each element is a pair containg
                   the MetaData URL and its EOID :  (MetaData_URL, EOID)
               aoi_toi is a tuple containing Area-of-interest
               Bounding-Box and the Time of Interest time range
        Each md_url is accessed in turn to get the metatada from the
        product facility.
        The MD is expected to contain a wcseo:EOCoverageSetDescription,
          with a number of coverageSetDescriptions
    """
    logger.info("Processing "+`len(md_urls)`+
                " EOCoverageSetDescription urls.")

    base_url = params['dsrc'] + \
        "?" + SERVICE_WCS + \
        '&version=' + service_version + \
        "&" + WCS_GET_COVERAGE +\
        "&" + WCS_IMAGE_FORMAT
    if params['download_subset']:
        base_url += \
            "&subset=Lat," +EPSG_4326+"("+`aoi_toi[0].ll[1]`+","+`aoi_toi[0].ur[1]`+")"+\
            "&subset=Long,"+EPSG_4326+"("+`aoi_toi[0].ll[0]`+","+`aoi_toi[0].ur[0]`+")"

    dl_reqests = []
    ndeocs = 0    # number of DescribeEOCoverageSet urls processed

    toteocs = float(len(md_urls))

    coastcache = None
    if should_check_coastline(params):
        shpfile = IE_30KM_SHPFILE
        prjfile = None
        coastcache = None
        try:
            coastcache = coastline_cache_from_aoi(shpfile, prjfile, aoi_toi[0])
        except Exception as e:
            logger.error("NOT checking coastline due to Error initialising coastline:\n"+`e`)

    for md_url_pair in md_urls:
        md_url = md_url_pair[0]
        eoid   = md_url_pair[1]
        if check_status_stopping(params["sc_id"]):
            raise StopRequest("Stop Request")

        #make sure percent_done is > 0
        percent_done = (float(ndeocs)/toteocs)*100.0
        if percent_done < 0.5:  percent_done = 1.0
        set_status(params["sc_id"], "Create DAR: get MD", percent_done)

        logger.info("Processing MD for EOID " + `eoid`)
        if 0 != DEBUG_MAX_DEOCS_URLS:
            if ndeocs>DEBUG_MAX_DEOCS_URLS: break
            ndeocs += 1

        dl_reqests += gen_dl_urls(
            params,
            aoi_toi,
            base_url,
            md_url,
            eoid,
            coastcache)

        if 0 != DEBUG_MAX_GETCOV_URLS and dl_reqests:
            dl_reqests = dl_reqests[:DEBUG_MAX_GETCOV_URLS]

    coastcache = None

    set_status(params["sc_id"], "Create DAR: get MD", 100)
    return dl_reqests


def generate_MD_urls(params, service_version, id_list):
    req_aoi = params["aoi_bbox"]
    ll = (req_aoi["lc"][0], req_aoi["lc"][1])
    ur = (req_aoi["uc"][0], req_aoi["uc"][1])

    base_url = params['dsrc'] + "?" + SERVICE_WCS + \
        '&version=' + service_version + \
        "&" + EOWCS_DESCRIBE_CS + \
        '&subset=phenomenonTime("'+params['from_date']+'","'+params['to_date'] + '")'+ \
        '&containment=overlaps' + \
        '&subset=Lat(' + `ll[1]`+','+`ur[1]`+')'\
        '&subset=Long('+ `ll[0]`+','+`ur[0]`+')'
    md_urls = []
    for dss_id in id_list:
        md_urls.append( (base_url + "&EOId=" + dss_id, dss_id) )
    return md_urls
    
def urls_from_OSCAT(params, eoids):
    raise IngestionError("Catalogues are not yet implemented")

def urls_from_EOWCS(params, eoids):

    caps = get_caps_from_pf(params['dsrc'])
    if None == caps:
        raise IngestionError("cannot get Capabilities from '"+params['dsrc']+"'")

    if check_status_stopping(params["sc_id"]):
        raise StopRequest("Stop Request")

    service_version = extract_ServiceTypeVersion(caps).strip()

    aoi_toi = build_aoi_toi(
        params["aoi_bbox"], params['from_date'], params['to_date'])
    
    if len(eoids) > 0:
        # use only the dssids specified, don't look for more.
        id_list = eoids
        caps = None  # no longer needed
    else:
        # find all datasets that match the bbox and Toi
        wcseo_dss = extract_DatasetSeriesSummaries(caps)
        caps = None  # no longer needed
        id_list = getDssList(params["sc_id"], wcseo_dss, aoi_toi)

    md_urls = generate_MD_urls(params, service_version, id_list)
    if IE_DEBUG>1:
        logger.debug("Qualified "+`len(md_urls)`+" md_urls")
    dl_requests = process_csDescriptions(
        params, aoi_toi, service_version, md_urls)
    return dl_requests


def get_download_URLs(params, eoids):
    if IE_DEBUG > 1:
        logger.debug ("   get_download_URLs: params=" + `params`)
    
    url_parts = params['dsrc'].split(":", 1)
    if len(url_parts) < 2:
        logger.error(
            "input error: malformed dsrc in request - no ':' found.")
        return None

    if url_parts[0] != 'http': 
        logger.error("Only http protocol is supported."+
             "Requested '"+url_parts[0]+"' is not supported.")
        return None

    urls = None
    if params['dsrc_type'] == DSRC_EOWCS_CHOICE:
        urls = urls_from_EOWCS(params, eoids)
    elif params['dsrc_type'] == DSRC_OSCAT_CHOICE:
        urls = urls_from_OSCAT(params, eoids)
    else:
        raise IngestionError("bad dsrc_type:" + params['dsrc_type'])

    return urls


def check_archived(scid, coverage_id):
    # returns True if matching metadata already exists in the archive.
    # Checks only for a match against the EO-ID (coverage ID)

    scenario = Scenario.objects.get(id=int(scid))
    archived = Archive.objects.filter(scenario=scenario,eoid=coverage_id)
    if len(archived)==0:
        if IE_DEBUG > 2:
            logger.info("Not in archive: " + `coverage_id`)
        return False

    return True


def download_urls(urls_with_dirs):
    dar = dar_builder.build_DAR(urls_with_dirs)
    dmcontroller = DownloadManagerController.Instance()
    status, dar_url, dm_dar_id = dmcontroller.submit_dar(dar)
    if status != "OK":
        raise DMError("DAR submit problem, status:" + status)
    return dar_url, dm_dar_id 


def request_download(sc_ncn_id, scid, urls):

    #create tmp dir for downloads
    full_path, rel_path = create_dl_dir(sc_ncn_id+"_")
    # set up the format of the subdirectory names
    id_digits = 3
    nreqs = len(urls)
    if nreqs>10000:  id_digits = 5
    if nreqs>1000:   id_digits = 4
    fmt = "p_"+sc_ncn_id+"_%0"+`id_digits`+"d"

    urls_with_dirs = []
    i = 1
    for url in urls:
        urls_with_dirs.append(  (os.path.join(rel_path, fmt % i), url) )
        i += 1
    urls = None

    dar_url, dm_dar_id = download_urls(urls_with_dirs)

    urls_with_dirs = None
    wfm_set_dar(scid, dm_dar_id)

    return full_path, dar_url, dm_dar_id

def stop_products_dl(product_list):
    logger.info("Stopping products download")
    dmcontroller = DownloadManagerController.Instance()
    dm_url = dmcontroller._dm_url

    for p in product_list:
        if "productProgress" in product:
            progress = product["productProgress"]
            if progress["status"] == "COMPLETED":
                # no point cancelling this one
                continue
        uuid = p['uuid']
        url = dm_url + DM_PRODUCT_CANCEL_TEMPLATE % uuid
        try:
            dm_response = json.loads(read_from_url(url))
        except urllib2.URLError as e:
            logger.warning("Error from DM while cancelling download: " + `e`)

def get_dar_list():
    dmcontroller = DownloadManagerController.Instance()
    dm_url = dmcontroller._dm_url
    url = dm_url+DM_DAR_STATUS_COMMAND
    waiting = True
    ts = time.time()
    tdiff = 0
    dar_status = []
    err = None
    while waiting:
        tdiff = time.time() - ts
        if tdiff > IE_DM_MAXWAIT2:
            raise DMError(
                "Unable to get DAR, timeout waiting for DM, "+`err`)
        try:
            dar_status = json.loads(read_from_url(url))
            waiting = False
        except urllib2.HTTPError as e:
            err = e
            time.sleep(2)

    if not "dataAccessRequests" in dar_status:
        raise DMError(
            "Bad DAR status from DM; no 'dataAccessRequests' found.")
    return dar_status["dataAccessRequests"]

def stop_active_dar_dl(active_dar_uuid):
    logger.info("Stopping active download, dar uuid="+`active_dar_uuid`)
    dar = get_dar_list()
    request = None
    for r in dar:
        if not "uuid" in r:
            continue
        uuid = r["uuid"]
        if uuid == active_dar_uuid:
            request = r
            break
    if not request:
        return
    if not "productList" in request:
        return 
    stop_products_dl(request["productList"])
    
def stop_download(scid, request):
    # nothing to do if no request
    if None==request:
        logger.warning("stop download: no dar request to process")
        return

    # do this first, in case someone else wants to also
    # cancel the dar.
    if not wfm_clear_dar(scid):
        # dar request has already been cleared before we got there
        logger.warning("stop download: dar had been cleared.")
        return
        
    # The DM does not have an interface to cancel an entire
    # DAR in one go, so we cancel all individual product
    # downloads in the DAR.
    if not "productList" in request:
        logger.warning("stop download: no productList in request")
        return 
    stop_products_dl(request["productList"])

def get_dar_status(dar_url):
    dar = get_dar_list()
    request = None
    for r in dar:
        if not "darURL" in r:
            continue
        if r["darURL"] == dar_url:
            request = r
            break
    return request

def wait_for_download(scid, dar_url, dar_id, ncn_id, max_wait=None):
    """
    scid may be None

    blocks until the DM reports that the DAR with this dar_url
    has completed all constituent individual product downloads
    """

    if None == ncn_id: ncn_id = "(None)"

    set_status(scid, "Downloading", 1)

    request = get_dar_status(dar_url)

    if check_status_stopping(scid):
        stop_download(scid, request)
        raise StopRequest("Stop Request")

    if None == request:
        # wait and try again
        time.sleep(DAR_STATUS_INTERVAL)
        request = get_dar_status(dar_url)
        if None == request:
            time.sleep(1)
            request = get_dar_status(dar_url)
            if check_status_stopping(scid):
                stop_download(scid, request)
                raise StopRequest("Stop Request")
        if None == request:
            time.sleep(1)
            request = get_dar_status(dar_url)
        if None == request:
            if None != scid: wfm_clear_dar(scid)
            raise DMError(
                "Bad DAR status from DM; no 'dataAccessRequests' found.")

    product_list = request["productList"]
    n_products = len(product_list)
    total_percent = n_products * 100
    all_done = False
    n_done = 0
    total_size = 0
    n_errors = 0
    failed_urls = []
    failed_dirs  = []
    try:
        ts = time.time()
        tdiff = 0
        last_status = {}
        last_st_message = ""
        while not all_done:
            tdiff = time.time() - ts
            all_done = True
            if None != max_wait and tdiff > max_wait:
                n_errors += 1
                logger.warning("Time-out waiting for download")
                break
            part_percent = 0
            n_done = 0
            n_errors = 0
            for product in product_list:
                if "productProgress" not in product:
                    continue

                dl_dir   = product["downloadDirectory"]
                progress = product["productProgress"]
                dl_status = progress["status"]

                if dl_status == "IN_ERROR":
                    if "message" in progress: msg = progress["message"]
                    else: msg = "(none)"
                    if "uuid" in product: uuid = product["uuid"]
                    else: uuid = "(unknown)"
                    if "productAccessUrl" in product:
                        url = product["productAccessUrl"]
                    else:
                        url = "(unknown)"
                    
                    if url not in failed_urls:
                        n_errors += 1
                        n_done   += 1
                        failed_urls.append(url)
                        failed_dirs.append( dl_dir )
                        logger.info("Dl Manager reports 'IN_ERROR' for uuid "+uuid+
                                    ", message: " + msg +
                                    "\n url: " + url)
                        dl_report = simplejson.dumps(product, indent=2)
                        logger.info("Dl Manager status: \n"+dl_report)

                elif dl_status == "COMPLETED":
                    n_done += 1

                else:
                    all_done = False

                if IE_DEBUG > 0:
                    prod_uuid = None
                    if 'uuid' in product:
                        prod_uuid = product['uuid']
                    else:
                        prod_uuid = 'unknown'
                    if not (prod_uuid in last_status and \
                            last_status[prod_uuid] == dl_status):
                        logger.debug("Status from DM: " + `dl_status` +
                                     ", prod. uuid="+`prod_uuid`)
                        last_status[prod_uuid] = dl_status

                if "progressPercentage" not in progress:
                    part_percent += 100
                else:
                    part_percent += progress["progressPercentage"]

                if "downloadedSize" in progress:
                    total_size += progress["downloadedSize"]

            percent_done = int( (float(part_percent)/float(total_percent))*100 )
            if percent_done < 1: percent_done = 1
            if all_done:
                if n_errors > 0:
                    set_status(scid, `n_errors` +" errors during Dl.", percent_done)
                else:
                    set_status(scid, "Finished Dl. ("+`n_products`+")", percent_done)
                if total_size < 102400:
                    ts = `total_size`+' bytes'
                else:
                    ts = `total_size/1024`+' kb'
                logger.info("Dl Manager reports downloaded "+ts+\
                                " in " + `n_products`+ ' products')
                break
            elif check_status_stopping(scid):
                stop_download(scid, request)
                raise StopRequest("Stop Request")
            else:
                status_message = "Downloading ("+`n_done`+'/'+`n_products`+")"
                set_status(scid, status_message, percent_done)
                new_st_message = ncn_id+" Status: "+status_message+" done: "+`percent_done`+"%"
                if new_st_message != last_st_message:
                    last_st_message = new_st_message
                    logger.info(new_st_message)

            if check_status_stopping(scid):
                stop_download(scid, request)
                raise StopRequest("Stop Request")

            sleep_time = DAR_STATUS_INTERVAL
            if tdiff > (32 * DAR_STATUS_INTERVAL):
                sleep_time = 5 * DAR_STATUS_INTERVAL
            elif tdiff > (6 * DAR_STATUS_INTERVAL):
                sleep_time = 2 * DAR_STATUS_INTERVAL
            time.sleep(sleep_time)
            request = get_dar_status(dar_url)

            if check_status_stopping(scid):
                stop_download(scid, request)
                raise StopRequest("Stop Request")
            
            product_list = request["productList"]


        # all done

        last_status = None

        if n_errors > 0:
            logger.info("Completed download with " + `n_errors` + " errors")
    
    except StopRequest:
        logger.info("StopRequest while waiting for download") 
        raise

    except Exception as e:
        logger.warning("Unexpected exception in wait_for_download: "+`e`)
        if IE_DEBUG > 0:
            traceback.print_exc(12,sys.stdout)
            raise e

    finally:
        if None != scid: wfm_clear_dar(scid)

    return n_errors, failed_dirs, failed_urls

# ----- the main entrypoint  --------------------------
def ingestion_logic(scid, scenario_data):
    root_dl_dir = DownloadManagerController.Instance()._download_dir
    custom = scenario_data['extraconditions']

    eoids = scenario_data['dssids']
    if scenario_data['dsrc_type'] != DSRC_EOWCS_CHOICE:
        logger.warning(
            'Data source type ' + scenario_data['dsrc_type'] +
            ' is not implemented')
    
    if not os.access(root_dl_dir, os.R_OK|os.W_OK):
        raise IngestionError("Cannot write/read "+root_dl_dir)

    if 0 != DEBUG_MAX_DEOCS_URLS:
        logger.info(" DEBUG_MAX_DEOCS_URLS  = "+`DEBUG_MAX_DEOCS_URLS`)
    if 0 != DEBUG_MAX_GETCOV_URLS:
        logger.info(" DEBUG_MAX_GETCOV_URLS = "+`DEBUG_MAX_GETCOV_URLS`)

    nreqs = 0
    retval = (0, None, None, None, "", None)
    scenario_data["sc_id"]  = scid
    scenario_data["custom"] = custom
    ncn_id = scenario_data["ncn_id"]
    dl_requests = get_download_URLs(scenario_data, eoids)
    if not dl_requests or 0 == len(dl_requests):
        logger.warning(`ncn_id`+": no GetCoverage requests generated")
        retval = (0, None, None, None, "NO_ACTION", None)
    else:
        if check_status_stopping(scid):
            raise StopRequest("Stop Request")

        nreqs = len(dl_requests)
        logger.info(`ncn_id`+": Submitting "+`nreqs`+" URLs to the Download Manager")
        dl_dir, dar_url, dar_id = \
            request_download(scenario_data["ncn_id"], scid, dl_requests)
        dl_errors, failed_dirs, failed_urls = wait_for_download(scid, dar_url, dar_id, ncn_id)
        if len(failed_urls) > 0:
            logger.warning("Failed downloads for "+`ncn_id`+":\n" +\
                                 '\n'.join(failed_urls))
                
        logger.info("Products for scenario " + ncn_id +
                    " downloaded to " + dl_dir)
        retval = (dl_errors, dl_dir, dar_url, dar_id, "OK", failed_dirs)

    return retval
