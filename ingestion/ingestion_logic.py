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
import urllib2
import xml.etree.ElementTree as ET
import xml.parsers.expat
import dar_builder
import json
import time

from osgeo import osr

import work_flow_manager

from utils import \
    Bbox, bbox_from_strings, TimePeriod, mkFname, DMError, \
    NoEPSGCodeError, IngestionError, StopRequest, \
    read_from_url, check_or_make_dir, make_new_dir

from settings import \
    IE_DEBUG, \
    DAR_STATUS_INTERVAL,\
    STOP_REQUEST

from dm_control import \
    DownloadManagerController, \
    DM_DAR_STATUS_COMMAND, \
    DM_PRODUCT_CANCEL_TEMPLATE

from models import \
    ScenarioStatus, \
    DSRC_EOWCS_CHOICE, \
    DSRC_OSCAT_CHOICE, \
    AOI_BBOX_CHOICE,   \
    AOI_POLY_CHOICE,   \
    AOI_SHPFILE_CHOICE


# For debugging:
# MAX_DEOCS_URLS limits the number of DescribeEOCoverageSet requests issued,
# MAX_GETCOV_URLS limits the number of GetCoverage requests generated
# 0 means unlimited
DEBUG_MAX_DEOCS_URLS  = 0
DEBUG_MAX_GETCOV_URLS = 0
if IE_DEBUG>0:
    DEBUG_MAX_DEOCS_URLS  = 0
    DEBUG_MAX_GETCOV_URLS = 0

# namespaces
wcs_vers = '2.0'
WCS_NS   = '{http://www.opengis.net/wcs/' + wcs_vers + '}'
WCSEO_NS = '{http://www.opengis.net/wcseo/1.0}'
OWS_NS   = '{http://www.opengis.net/ows/2.0}'
GML_NS   = '{http://www.opengis.net/gml/3.2}'
GMLCOV_NS= '{http://www.opengis.net/gmlcov/1.0}'
EOP_NS   = '{http://www.opengis.net/eop/2.0}'
OM_NS    = '{http://www.opengis.net/om/2.0}'

# CRS
EPSG_4326 = 'http://www.opengis.net/def/crs/EPSG/0/4326'

# misc. constants
SERVICE_WCS        = "service=wcs"
WCS_GET_CAPS       = "request=GetCapabilities"
EOWCS_DESCRIBE_CS  = "request=DescribeEOCoverageSet"
WCS_GET_COVERAGE   = "request=GetCoverage"
WCS_FORMATS        = "format=image/tiff&mediatype=multipart/mixed"

DEFAULT_SERVICE_VERSION = "2.0.1"

EXCEPTION_TAG      = "ExceptionReport"
CAPABILITIES_TAG   = "Capabilities"
EOCS_DESCRIPTION_TAG = "EOCoverageSetDescription"

# sensor type XML example:
# <gmlcov:metadata>
#   <gmlcov:Extension>
#     <wcseo:EOMetadata>
#       <eop:EarthObservation gml:id="some_id"
#        xsi:schemaLocation="http://www.opengis.net/opt/2.0 ../xsd/opt.xsd">
#         <om:procedure>
#           <eop:EarthObservationEquipment gml:id="some_id">  
#             <eop:sensor>
#               <eop:Sensor>
#                 <eop:sensorType>OPTICAL</eop:sensorType>
#               </eop:Sensor>
#             </eop:sensor>
#           </eop:EarthObservationEquipment>
#         </om:procedure>
#
SENSOR_XPATH = \
    GMLCOV_NS + "metadata/" + \
    GMLCOV_NS + "Extension/" + \
    WCSEO_NS  + "EOMetadata/" + \
    EOP_NS    + "EarthObservation/" + \
    OM_NS     + "procedure/"        + \
    EOP_NS    + "EarthObservationEquipment/" + \
    EOP_NS    + "sensor/" + \
    EOP_NS    + "Sensor/" + \
    EOP_NS    + "sensorType"
    


logger = logging.getLogger('dream.file_logger')

# ------------ osr Init  ------------
SPATIAL_REF_WGS84 = osr.SpatialReference()
SPATIAL_REF_WGS84.SetWellKnownGeogCS( "EPSG:4326" )


# ------------ spatial references cache --------------------------

spatial_refs = {}

def get_spatialReference(epsg):
    ref = None
    try:
        ref = spatial_refs[epsg]
    except KeyError:
        ref = osr.SpatialReference()
        ret = ref.ImportFromEPSG(epsg)
        if 0 != ret:
            raise NoEPSGCodeError("Unknown EPSG code "+epsg)
        spatial_refs[epsg] = ref
    return ref


# ------------ utilities --------------------------

def check_status_stopping(scid):
    status = ScenarioStatus.objects.get(scenario_id=scid).status
    return status == STOP_REQUEST

def wfm_set_dar(scid, darid):
    work_flow_manager.WorkFlowManager.Instance().set_active_dar(scid, darid)

def wfm_clear_dar(scid):
    work_flow_manager.WorkFlowManager.Instance().set_active_dar(scid, '')

def srsName_to_Number(srsName):
    if not srsName.startswith("http://www.opengis.net/def/crs/EPSG"):
        raise NoEPSGCodeError("Unknown SRS: '" + srsName +"'")
    return int(srsName.split('/')[-1])

def file_is_zero(fname):
    return 0 == os.stat(fname)[stat.ST_SIZE]

def bbox_to_WGS84(epsg, bbox):
    if 4326==epsg: return
    srcRef = get_spatialReference(epsg)
    ct=osr.CoordinateTransformation(srcRef, SPATIAL_REF_WGS84)
    new_ll = ct.TransformPoint(bbox.ll[0], bbox.ll[1])
    new_ur = ct.TransformPoint(bbox.ur[0], bbox.ur[1])
    bbox.ll = (new_ll[0], new_ll[1])
    bbox.ur = (new_ur[0], new_ur[1])

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

# ------------ XML parsing --------------------------
def is_nc_tag(qtag, nctag):
    parts=qtag.split("}")
    if len(parts)==1:
        return qtag==nctag
    elif len(parts)==2:
        return nctag==parts[1]
    else:
        return False

def tree_is_exception(tree):
    return is_nc_tag(tree.tag,EXCEPTION_TAG)


def parse_file(src_data, expected_root, url):
    result = None
    try:
        xmlTree = ET.parse(src_data)
        result = xmlTree.getroot()
        if tree_is_exception(result):
            result = None
            if IE_DEBUG > 0:
                logger.warning("'"+url+"' contains exception")
                if IE_DEBUG > 0:
                    logger.info(ET.tostring(result))
        elif not is_nc_tag(result.tag, expected_root):
            result = None
            logger.error("'"+url+"' does not contain expected root '"+ \
                   expected_root )
    except IOError as e:
        loger.error("Cannot open/parse md source '"+url+"': " + `e`)
        return None
    except xml.parsers.expat.ExpatError as e:
        logger.error("Cannot parse '"+url+"', error="+`e`)
        return None
    except:
        logger.error("Cannot parse '"+url+"', unknown error.")
        return None

    return result


def extract_path_text(cd, path):
    ret = None
    leaf_node = cd.find("./"+path)
    if None == leaf_node:
        return None
    return leaf_node.text


def extract_Id(dss):
    dsid = dss.find("./"+ WCSEO_NS + "DatasetSeriesId")
    if None == dsid:
        logger.error("'DatasetSeriesId' not found in DatasetSeriesSummary")
        return None
    return dsid.text


def is_x_axis_first(axisLabels):
    labels = axisLabels.strip().lower().split(' ')
    if len(labels) != 2:
        logger.error("Error: can't parse axisLabels '"+axisLabels+"'")
        return False
    if labels[0] == 'lat' or labels[0] == 'y':
        return False
    if labels[0] == 'long' or labels[0] == 'x':
        return True
    else:
        logger.error("Error: can't parse axisLabels '"+axisLabels+"'")
        return False

def extract_gml_bbox(cd):
    # cd is the CoverageDescription, should contain boundedBy/Envelope
    # The extracted bbox is converted to WGS84
    envelope = cd.find("./" + GML_NS + "boundedBy" + "/" \
                            +  GML_NS + "Envelope" )
    if None == envelope:
        return None

    srsNumber = None
    axisLabels = None
    try:
        axisLabels = envelope.attrib['axisLabels']
        srsName = envelope.attrib['srsName']
        srsNumber = srsName_to_Number(srsName)
    except KeyError:
        logger.error("Error: srsName or axisLabels not found")
        return None
    except NoEPSGCodeError as e:
        logger.error("Error: "+e)
        return None

    lc = envelope.find("./"+ GML_NS +"lowerCorner")
    uc = envelope.find("./"+ GML_NS +"upperCorner")

    if None==lc or None==uc:
        logger.error(
            "Error: lowerCorner or upperCorner not found in envelope.")
        return None

    bb = bbox_from_strings(lc.text, uc.text, is_x_axis_first(axisLabels))
    bbox_to_WGS84(srsNumber, bb)
    return bb


def extract_WGS84bbox(dss):
    WGS84bbox = dss.find("./"+ OWS_NS +"WGS84BoundingBox")
    if None == WGS84bbox:
        logger.error("'WGS84BoundingBox' not found in DatasetSeriesSummary")
        return None
    lc = WGS84bbox.find("./"+ OWS_NS +"LowerCorner")
    uc = WGS84bbox.find("./"+ OWS_NS +"UpperCorner")
    if None == lc or None == uc:
        logger.error("error, LowerCorner or Upper Corner not found in bbox")
        return None
    return bbox_from_strings(lc.text, uc.text)


def extract_TimePeriod(dss):
    # returns an instance of a utils.TimePeriod
    tp = dss.find("./"+ GML_NS + "TimePeriod")
    if None == tp: return None
    begin_pos = tp.find("./"+ GML_NS + "beginPosition")
    end_pos   = tp.find("./"+ GML_NS + "endPosition")
    if None == begin_pos or None == end_pos: return None
    return TimePeriod(begin_pos.text, end_pos.text)

def extract_om_time(cd):
    phenomenonTime = cd.find("./"+GMLCOV_NS+"metadata" + "/" \
                                 +GMLCOV_NS+"Extension" + "/" \
                                 +WCSEO_NS+"EOMetadata" + "/" \
                                 +EOP_NS+"EarthObservation" + "/" \
                                 +OM_NS+"phenomenonTime")
    if None==phenomenonTime:
        logger.error("Error: failed to find 'phenomenonTime'")
        return None
    return extract_TimePeriod(phenomenonTime)
    

def extract_ServiceTypeVersion(caps):
    stv = caps.findall("./"+ OWS_NS +"ServiceIdentification" +
                          "/" + OWS_NS +"ServiceTypeVersion")
    if len(stv) < 1:
        logger.warning("ServiceTypeVersion not found")
        return DEFAULT_SERVICE_VERSION
    return stv[0].text

def extract_DatasetSeriesSummary(caps):
    result = []
    wcs_extension = caps.findall(
        "." +
        "/" + WCS_NS +"Contents" +
        "/" + WCS_NS + "Extension")
    if len(wcs_extension) < 1:
        logger.error("Contents/Extension not found")
    else:
        result = wcs_extension[0].findall(
        "./" + WCSEO_NS +"DatasetSeriesSummary")
    return result

def extract_CoverageId(cd):
    covId = None
    coverageIdNode = cd.find("./"+WCS_NS+"CoverageId")
    if None!=coverageIdNode:
        covId = coverageIdNode.text
    else:
        try:
            covId = cd.attrib[GML_NS+'id']
        except KeyError:
            pass
    return covId

# ------------ status reporting  -------------------
def set_status(sc_id, st_text, percentage):
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
        if check_status_stopping(scid):
            raise StopRequest("Stop Request")

        bb1 = extract_WGS84bbox(dss)
        if None == bb1:
            logger.warning("Failed to extract bb from " + `dss`)
            continue
        timeperiod = extract_TimePeriod(dss)
        if None == timeperiod:
            logger.warning("Failed to extract time range from " + `dss`)
        if bb1.overlaps( req_bb ) and req_time.overlaps(timeperiod):
            id_list.append( extract_Id(dss) )

    return id_list

def check_bbox(coverageDescription, req_bbox):
    bb = extract_gml_bbox(coverageDescription)
    if None == bb:
        return False
    return bb.overlaps(req_bbox)

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


def check_sensor_type(cd, req):
    if not 'sensor_type' in req:
        return True
    req_sensor = req['sensor_type']
    md_sensor  = extract_path_text(cd, SENSOR_XPATH)
    return req_sensor == md_sensor

def gen_getCov_params(params, aoi_toi, md_url):
    if IE_DEBUG > 0:
        logger.debug("Generating getcoverage params from ULR '"+md_url+"'")

    ret = []

    (fp, cd_tree) = getXmlTree(md_url, EOCS_DESCRIPTION_TAG)
    if check_status_stopping(params['sc_id']):
        raise StopRequest("Stop Request")

    if None==cd_tree:
        if None != fp: fp.close()
        return ret

    if IE_DEBUG > 1:
        try:
            nreturned = cd_tree.attrib['numberReturned']
            nmatched  = cd_tree.attrib['numberMatched']
            logger.info("    nreturned = "+`nreturned`+\
                            ", nmatched =  "+`nmatched`)
        except KeyError:
            pass
    
    cds = cd_tree.findall("./" + \
                              WCS_NS + "CoverageDescriptions" + "/" +\
                              WCS_NS + "CoverageDescription")
    if len(cds) < 1:
        logger.warning("No CoverageDescriptions found in '"+md_url+"'")

    passed = 0
    for cd in cds:

        if check_status_stopping(params["sc_id"]):
            raise StopRequest("Stop Request")

        if not check_bbox(cd, aoi_toi[0]):
            print "BBOX failed"
            if IE_DEBUG > 2: logger.info("  bbox check failed.")
            continue
        
        if not check_timePeriod(cd, aoi_toi[1], md_url):
            if IE_DEBUG > 2: logger.info("  TimePeriod check failed.")
            continue

        if not check_sensor_type(cd, params): 
            if IE_DEBUG > 2: logger.info("  sensor type check failed.")
            continue

        if not check_custom_conditions(cd, params):
            if IE_DEBUG > 2: logger.info("  custom conds check failed.")
            continue

        passed = passed+1
        coverageId = extract_CoverageId(cd)
        if None == coverageId:
            logger.error("Cannot find CoverageId in '"+md_url+"'")
            continue
        if IE_DEBUG > 2: logger.info("  coverageId="+coverageId)
        ret.append("CoverageId="+coverageId)

    if None != fp: fp.close()
    cd_tree = None
    if IE_DEBUG > 1:
        logger.info( "conditions passed: "+`passed`+" / "+`len(cds)`)
        
    return ret
    

def process_csDescriptions(params, aoi_toi, service_version, md_urls):
    logger.info("Processing "+`len(md_urls)`+" coverageSetDescriptions")
    subset_str = \
        "subset=Lat," +EPSG_4326+"("+`aoi_toi[0].ll[1]`+","+`aoi_toi[0].ur[1]`+")"+\
        "&subset=Long,"+EPSG_4326+"("+`aoi_toi[0].ll[0]`+","+`aoi_toi[0].ur[0]`+")"
    base_url = params['dsrc'] + \
        "?" + SERVICE_WCS + \
        '&version=' + service_version + \
        "&" + WCS_GET_COVERAGE + \
        "&" + WCS_FORMATS
    gc_requests = []
    ndeocs = 1    # number of DescribeEOCOverageSet urls processed
    ngc    = 1
    toteocs = float(len(md_urls))

    for md_url in md_urls:
        if check_status_stopping(params["sc_id"]):
            raise StopRequest("Stop Request")

        set_status(params["sc_id"],
                   "Create DAR: get MD",
                   (float(ndeocs)/toteocs)*100.0)
        if 0 != DEBUG_MAX_DEOCS_URLS:
            if ndeocs>DEBUG_MAX_DEOCS_URLS: break
            ndeocs += 1
        getCov_params = gen_getCov_params(params, aoi_toi, md_url)
        for gc_fragment in getCov_params:
            if 0 != DEBUG_MAX_GETCOV_URLS:
                if ngc>DEBUG_MAX_GETCOV_URLS: break
                ngc += 1
            gc_requests.append( base_url +"&"+ gc_fragment +"&"+ subset_str)

    return gc_requests


def extract_aoi_toi(params):
    # extract bbox and toi from request input
    req_aoi = params["aoi_bbox"]
    req_bb_ll = ( float(req_aoi["lc"][0]), float(req_aoi["lc"][1]) )
    req_bb_ur = ( float(req_aoi["uc"][0]), float(req_aoi["uc"][1]) )
    req_bb = Bbox( req_bb_ll, req_bb_ur )

    req_time = TimePeriod(params['from_date'], params['to_date'])

    return req_bb, req_time


def getMD_urls(params, service_version, id_list):
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
        md_urls.append( base_url + "&EOId=" + dss_id )
    return md_urls
    
def urls_from_OSCAT(params, eoids):
    raise IngestionError("Catalogues are not yet implemented")

def urls_from_EOWCS(params, eoids):
    base_url = params['dsrc'] + "?" + SERVICE_WCS
    url_GetCapabilities = base_url + "&" + WCS_GET_CAPS
    (fp, caps) = getXmlTree(url_GetCapabilities, CAPABILITIES_TAG)
    if None == caps:
        logger.error("Cannot parse getCap file. Url="+url_GetCapabilities)
        if None != fp: fp.close()
        if check_status_stopping(params["sc_id"]):
            raise StopRequest("Stop Request")
        return None

    service_version = extract_ServiceTypeVersion(caps).strip()
    wcseo_dss = extract_DatasetSeriesSummary(caps)

    caps = None  # no longer needed
    fp.close()

    if check_status_stopping(params["sc_id"]):
        raise StopRequest("Stop Request")

    aoi_toi = extract_aoi_toi(params)
    id_list = getDssList(params["sc_id"], wcseo_dss, aoi_toi)

    if IE_DEBUG>1:
        logger.debug(" id list before culling:" + `id_list`)
    
    # cull id list according to eoids specified by user
    if len(eoids) >  0:
        culled_list = []
        for e in eoids:
            if e in id_list:
                culled_list.append(e)
        id_list = culled_list
        if IE_DEBUG>1:
            logger.debug(" id list after culling:" + `id_list`)

    md_urls = getMD_urls(params, service_version, id_list)
    if IE_DEBUG>1:
        logger.debug("Qualified "+`len(md_urls)`+" md_urls")
    gc_requests = process_csDescriptions(
        params, aoi_toi, service_version, md_urls)
    return gc_requests

    
def get_coverage_URLs(params, eoids):
    if IE_DEBUG > 1:
        print "   get_coverage_URLs: params=" + `params`
    
    url_parts = params['dsrc'].split(":", 1)
    if len(url_parts) < 2:
        logger.error(
            "input error: malformed dsrc in request - no ':' found.")
        return None

    if url_parts[0] != 'http': 
        logger.error("Only http protocol is supported."+
             "Requested '"+url_parts[0]+"' is not supported.")
        return None

    if params['dsrc_type'] == DSRC_EOWCS_CHOICE:
        return urls_from_EOWCS(params, eoids)
    elif params['dsrc_type'] == DSRC_OSCAT_CHOICE:
        return urls_from_OSCAT(params, eoids)
    else:
        raise IngestionError("bad dsrc_type:" + params['dsrc_type'])

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
    dar = dar_builder.build_DAR(urls_with_dirs)
    urls_with_dirs = None

    dmcontroller = DownloadManagerController.Instance()
    status, dar_url, dm_dar_id = dmcontroller.submit_dar(dar)
    if status != "OK":
        raise DMError("DAR submit problem, status:" + status)
    wfm_set_dar(scid, dm_dar_id)

    return full_path, dar_url, dm_dar_id

def stop_products_dl(product_list):
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
    try:
        dar_status = json.loads(read_from_url(url))
    except urllib2.URLError as e:
        raise DMError("Unable to get DAR status from DM, error=" + `e`)
    if not "dataAccessRequests" in dar_status:
        raise DMError(
            "Bad DAR status from DM; no 'dataAccessRequests' found.")
    return dar_status["dataAccessRequests"]

def stop_active_dar_dl(active_dar_uuid):
    dar = get_dar_list()
    request = None
    for r in dar:
        if not "uuid" in r:
            continue
        uuid = r["uuid"]
        if uuid == active_dar_uuid:
            request = r
            break
    if not "productList" in request:
        return 
    stop_products_dl(request["productList"])
    
def stop_download(scid, request):
    # nothing to do if no request
    if None==request:
        return

    # do this first, in case someone else wants to also
    # cancel the dar.
    if not wfm_clear_dar(scid):
        # dar request has already been cleared before we got there
        return
        
    # The DM does not have an interface to cancel an entire
    # DAR in one go, so we cancel all individual product
    # downloads in the DAR.
    if not "productList" in request:
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

def wait_for_download(scid, dar_url, dar_id):
    """ blocks until the DM reports that the DAR with this dar_url
        has completed all constituent individual product downloads
    """
    set_status(scid, "Downloading", 1)

    request = get_dar_status(dar_url)
    if None == request:
        # wait and try again
        time.sleep(DAR_STATUS_INTERVAL)
        request = get_dar_status(dar_url)
        if None == request:
            time.sleep(1)
            request = get_dar_status(dar_url)
        if None == request:
            time.sleep(1)
            request = get_dar_status(dar_url)
        if None == request:
            wfm_clear_dar(scid)
            raise DMError(
                "Bad DAR status from DM; no 'dataAccessRequests' found.")

    if check_status_stopping(scid):
        stop_download(scid, request)
        raise StopRequest("Stop Request")

    product_list = request["productList"]
    n_products = len(product_list)
    total_percent = n_products * 100
    all_done = False
    try:
        while not all_done:
            all_done = True
            part_percent = 0
            for product in product_list:
                if "productProgress" not in product:
                    continue
                progress = product["productProgress"]
                if progress["status"] != "COMPLETED":
                    all_done = False
                if "progressPercentage" not in progress:
                    part_percent += 100
                else:
                    part_percent += progress["progressPercentage"]
        
            percent_done = int( (float(part_percent)/float(total_percent))*100 )
            if percent_done < 1: percent_done = 1
            if all_done:
                set_status(scid, "Done ("+`n_products`+")", percent_done)
                break
            elif check_status_stopping(scid):
                stop_download(scid, request)
                raise StopRequest("Stop Request")
            else:
                set_status(scid, "Downloading ("+`n_products`+")", percent_done)
            time.sleep(DAR_STATUS_INTERVAL)
            request = get_dar_status(dar_url)
            if check_status_stopping(scid):
                stop_download(scid, request)
                raise StopRequest("Stop Request")
            
            product_list = request["productList"]
    except Exception as e:
        logger.warning("Unexpected exception in wait_for_download: "+`e`)
        if IE_DEBUG > 0:
            traceback.print_exc(12,sys.stdout)
    finally:
        wfm_clear_dar(scid)

# ----- the main entrypoint  --------------------------
def ingestion_logic(scid,
                    scenario_data,
                    eoids,
                    custom):
    root_dl_dir = DownloadManagerController.Instance()._download_dir

    if not os.access(root_dl_dir, os.R_OK|os.W_OK):
        raise IngestionError("Cannot write/read "+root_dl_dir)

    if 0 != DEBUG_MAX_DEOCS_URLS:
        logger.info(" DEBUG_MAX_DEOCS_URLS  = "+`DEBUG_MAX_DEOCS_URLS`)
    if 0 != DEBUG_MAX_GETCOV_URLS:
        logger.info(" DEBUG_MAX_GETCOV_URLS = "+`DEBUG_MAX_GETCOV_URLS`)

    nreqs = 0
    retval = (None, None, None)
    
    scenario_data["sc_id"]  = scid
    scenario_data["custom"] = custom
    gc_requests = get_coverage_URLs(scenario_data, eoids)
    if not gc_requests or 0 == len(gc_requests):
        logger.warning(" no GetCoverage requests generated")
    else:
        if check_status_stopping(scid):
            raise StopRequest("Stop Request")

        nreqs = len(gc_requests)
        logger.info("Submitting "+`nreqs`+" URLs to the Download Manager")
        dl_dir, dar_url, dar_id = \
            request_download(scenario_data["ncn_id"], scid, gc_requests)
        wait_for_download(scid, dar_url, dar_id)
        logger.info("Products for scenario " + scenario_data["ncn_id"]+
                    " downloaded to " + dl_dir)
        retval = (dl_dir, dar_url, dar_id)

    return retval
