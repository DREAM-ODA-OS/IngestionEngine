############################################################
#  Project: DREAM
#  Module:  Task 5 ODA Ingestion Engine 
#  Contribution: Milan Novacek (CVC)
#  Date:    Aug 20, 2013
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

from osgeo import osr
from utils import Bbox, bbox_from_strings, TimePeriod, mkFname, \
    NoEPSGCodeError, IngestionError
from settings import IE_DEBUG, IE_DOWNLOAD_DIR

# For debugging:
# used to limit the number of DescribeEOCoverageSet requests issued
# 0 means unlimited
DEBUG_MAX_DEOCS_URLS = 2

# For debugging:
# used to limit the number of GetCoverage requests generated
# 0 means unlimited
DEBUG_MAX_GETCOV_URLS = 3

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

def getDssList(eo_dss_list, req_bb, req_time):
    # get list of datasets that overlap bbox
    id_list = []
    for dss in eo_dss_list:
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

def check_custom_conditions(req, cd):
    # TODO
    return True

def gen_getCov_params(params, aoi_toi, md_url):
    if IE_DEBUG > 0:
        logger.debug("Generating getcoverage params from ULR '"+md_url+"'")

    ret = []

    (fp, cd_tree) = getXmlTree(md_url, EOCS_DESCRIPTION_TAG)

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
        if not check_bbox(cd, aoi_toi[0]):               continue
        if not check_timePeriod(cd, aoi_toi[1], md_url): continue
        if not check_custom_conditions(params, cd):      continue
        passed = passed+1
        coverageId = extract_CoverageId(cd)
        if None == coverageId:
            logger.error("Cannot find CoverageId in '"+md_url+"'")
            continue
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
    for md_url in md_urls:
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


def getProducts(params):
    pass

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
    

def getGetCoverageURLs(params):
    if IE_DEBUG > 1:
        print "   getMDList: params=" + `params`
    
    url_parts = params['dsrc'].split(":", 1)
    if len(url_parts) < 2:
        logger.error(
            "input error: malformed dsrc in request - no ':' found.")
        return None

    if url_parts[0] != 'http': 
        logger.error("Only http protocol is supported."+
             "Requested '"+url_parts[0]+"' is not supported.")
        return None

    base_url = params['dsrc'] + "?" + SERVICE_WCS
    url_GetCapabilities = base_url + "&" + WCS_GET_CAPS
    (fp, caps) = getXmlTree(url_GetCapabilities, CAPABILITIES_TAG)
    if None == caps:
        logger.error("Cannot parse getCap file. Url="+url_GetCapabilities)
        if None != fp: fp.close()
        return None
        
    service_version = extract_ServiceTypeVersion(caps).strip()

    # set up bbox from request input
    req_aoi = params["aoi_bbox"]
    req_bb_ll = ( float(req_aoi["lc"][0]), float(req_aoi["lc"][1]) )
    req_bb_ur = ( float(req_aoi["uc"][0]), float(req_aoi["uc"][1]) )
    req_bb = Bbox( req_bb_ll, req_bb_ur )

    req_time = TimePeriod(params['from_date'], params['to_date'])

    aoi_toi = (req_bb, req_time)
    wcseo_dss = extract_DatasetSeriesSummary(caps)
    id_list = getDssList(wcseo_dss, req_bb, req_time)

    caps = None  # no longer needed
    fp.close()

    md_urls = getMD_urls(params, service_version, id_list)
    if IE_DEBUG>1:
        logger.debug("Qualified "+`len(md_urls)`+" md_urls")
    gc_requests = process_csDescriptions(
        params, aoi_toi, service_version, md_urls)
    return gc_requests

def invoke_dm(sc_id, urls):
    #create tmp dir for downloads
    dl_dir = os.path.join(IE_DOWNLOAD_DIR,mkFname(sc_id+"_"))
    try:
        os.mkdir(dl_dir,0740)
        logger.info("Created "+dl_dir)
    except OSError as e:
        logger.error("Failed to create "+dl_dir+": "+`e`)
        raise
    
    # TODO DEVELOP TEMP ONLY - DELETE THIS:
    print "---- TEMP for developent: downloading first 2 ulrs ----"
    print "    (TBD: to be passed to the download manager)"
    i = 0
    fn_base = "prod_"
    precision = 3
    nreqs = len(urls)
    if len>10000:  precision = 5
    if len>1000:   precision = 4
    blk_sz = 8192
    fmt = "%0"+`precision`+"d"
    for rr in urls[0:2]:
        buffer = None
        i += 1
        fn = fn_base + fmt % i
        print "downloading: "+ rr
        f = None
        try:
            f = open(os.path.join(dl_dir, fn), "wb")
            r = urllib2.urlopen( rr )
            while True:
                buffer = r.read(blk_sz)
                if not buffer:
                    break
                f.write(buffer)
        except urllib2.URLError as e:
            logger.error("Download failed: " + `e`)
        finally:
            if f!=None: f.close()

    return dl_dir

# ----- the main entrypoint  --------------------------
def ingestion_logic(scenario_data):
    if not os.access(IE_DOWNLOAD_DIR, os.R_OK|os.W_OK):
        raise IngestionError("Cannot write/read "+IE_DOWNLOAD_DIR)

    if 0 != DEBUG_MAX_DEOCS_URLS:
        logger.info(" DEBUG_MAX_DEOCS_URLS  = "+`DEBUG_MAX_DEOCS_URLS`)
    if 0 != DEBUG_MAX_GETCOV_URLS:
        logger.info(" DEBUG_MAX_GETCOV_URLS = "+`DEBUG_MAX_GETCOV_URLS`)

    for k in scenario_data.keys():
        print "  "+k+":\t\t  "+`scenario_data[k]`
    print

    nreqs = 0
    retval = None
    gc_requests = getGetCoverageURLs(scenario_data)
    if None==gc_requests:
        logger.warning(" no GetCoverage requests generated")
    else:
        nreqs = len(gc_requests)
        logger.info("Sending "+`nreqs`+" URLs to the Download Manager")
        retval = invoke_dm(scenario_data["ncn_id"], gc_requests)

    return retval
