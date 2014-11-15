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
#  Ingestion Engine: xml and  metadata parser utility
#
############################################################

import logging
import xml.etree.ElementTree as ET
import xml.parsers.expat


from utils import DummyLogger

if __name__ == '__main__':
    IE_DEBUG=2
    logger = DummyLogger()
else:
    from settings import IE_DEBUG
    logger = logging.getLogger('dream.file_logger')

from utils import \
    Bbox, \
    bbox_to_WGS84, \
    bbox_from_strings, \
    coords_from_text, \
    TimePeriod, \
    NoEPSGCodeError, \
    IngestionError

# WCS types
CRS_URI_DRAFT201  = 'http://www.opengis.net/wcs/service-extension/crs/1.0'
CRS_URI_FINAL201  = 'http://www.opengis.net/wcs/crs/1.0'
WCS_TYPE_UNKNOWN  = 0
WCS_TYPE_DRAFT201 = 199
WCS_TYPE_FINAL201 = 201

# namespaces
wcs_vers = '2.0'
WCS_NS   = '{http://www.opengis.net/wcs/' + wcs_vers + '}'
WCSEO_NS = '{http://www.opengis.net/wcseo/1.0}'
OWS_NS   = '{http://www.opengis.net/ows/2.0}'
GML_NS   = '{http://www.opengis.net/gml/3.2}'
GMLCOV_NS= '{http://www.opengis.net/gmlcov/1.0}'
EOP_NS   = '{http://www.opengis.net/eop/2.0}'
OM_NS    = '{http://www.opengis.net/om/2.0}'
OPT_NS   = '{http://www.opengis.net/opt/2.0}' 

XLINK_NS = '{http://www.w3.org/1999/xlink}'

EXCEPTION_TAG      = "ExceptionReport"
DEFAULT_SERVICE_VERSION = "2.0.1"

#<gml:boundedBy>
#<gml:Envelope axisLabels="lat long" 
#   srsDimension="2" srsName="http://www.opengis.net/def/crs/EPSG/0/4326" 
#   uomLabels="deg deg">
#   <gml:lowerCorner>44.14 0.8</gml:lowerCorner>
#   <gml:upperCorner>44.15 0.9</gml:upperCorner>
# </gml:Envelope></gml:boundedBy>

# <gmlcov:metadata>
#   <gmlcov:Extension>
#     <wcseo:EOMetadata>
#       <eop:EarthObservation
#            gml:id="eop_L930564_20110119_L5_199_030_USGS_surf_pente_30m_RGB_WSG84"
#            xsi:schemaLocation="http://www.opengis.net/opt/2.0 ../xsd/opt.xsd">
#         <om:phenomenonTime>
#           <gml:TimePeriod
#             gml:id="tp_L930564_20110119_L5_199_030_USGS_surf_pente_30m_RGB_WSG84">
#               <gml:beginPosition>2011-01-19T00:00:00</gml:beginPosition>
#               <gml:endPosition>2011-01-19T00:00:00</gml:endPosition>
#           </gml:TimePeriod>
#         </om:phenomenonTime>

# <gmlcov:Extension>
#   <wcseo:EOMetadata>
#   <eop:EarthObservation
#       gml:id="eop_L930564_20110119_L5_199_030_USGS_surf_pente_30m_RGB_WSG84"
#       xsi:schemaLocation="http://www.opengis.net/opt/2.0 ../xsd/opt.xsd">
#     <eop:metaDataProperty>
#       <eop:EarthObservationMetaData>
#         <eop:identifier>L930564_20110119_L5_199_030_USGS_surf_pente_30m_RGB_WSG84</eop:identifier>
#       </eop:EarthObservationMetaData>
#     </eop:metaDataProperty>
#   </eop:EarthObservation>

EO_METADATA_XPATH = \
    GMLCOV_NS + "metadata/" + \
    GMLCOV_NS + "Extension/" + \
    WCSEO_NS  + "EOMetadata/"

EO_EARTHOBSERVATION_XPATH = EO_METADATA_XPATH + EOP_NS+"EarthObservation/"

EO_PHENOMENONTIME = \
    EO_EARTHOBSERVATION_XPATH + \
    OM_NS+"phenomenonTime"

EO_IDENTIFIER_XPATH = \
    EO_EARTHOBSERVATION_XPATH + \
    EOP_NS+"metaDataProperty/" + \
    EOP_NS+"EarthObservationMetaData/" + \
    EOP_NS+"identifier/"

EO_EQUIPMENT_XPATH = \
    EO_EARTHOBSERVATION_XPATH + \
    OM_NS    + "procedure/"        + \
    EOP_NS   + "EarthObservationEquipment/"

# cloud cover XML example
# <!-- cloud cover -->
# <gmlcov:metadata>
#   <gmlcov:Extension>
#     <wcseo:EOMetadata>
#       <eop:EarthObservation gml:id="b57ea609-">
#         <om:result>
#           <opt:EarthObservationResult gml:id="uuid_94567f">
#             <opt:cloudCoverPercentage uom="%">13.25</opt:cloudCoverPercentage>
#           </opt:EarthObservationResult>
#         </om:result>
#       </eop:EarthObservation>
#     </wcseo:EOMetadata>
#   </gmlcov:Extension>
# </gmlcov:metadata>

EORESULT_XPATH = \
    EO_METADATA_XPATH + \
    EOP_NS   + "EarthObservation/" + \
    OM_NS    + "result/"           + \
    OPT_NS   + "EarthObservationResult"

CLOUDCOVER_XPATH = \
    EORESULT_XPATH + '/' + \
    OPT_NS   + "cloudCoverPercentage"
   

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
    EO_EQUIPMENT_XPATH +\
    EOP_NS   + "sensor/" + \
    EOP_NS   + "Sensor/" + \
    EOP_NS   + "sensorType"
    
# acquisition angle XML example
# <!-- acquisition angle -->
# <gmlcov:metadata>
#   <gmlcov:Extension>
#     <wcseo:EOMetadata>
#
#       <eop:EarthObservation gml:id="some_id" >
#         <om:procedure>
#           <eop:EarthObservationEquipment gml:id="some_id">
#             <eop:acquisitionParameters>
#               <eop:Acquisition>
#                 <eop:incidenceAngle uom="deg">+7.23391641</eop:incidenceAngle>
#               </eop:Acquisition>
#             </eop:acquisitionParameters>
#           </eop:EarthObservationEquipment>
#         </om:procedure>
#       </eop:EarthObservation>

INCIDENCEANGLE_XPATH = \
    EO_EQUIPMENT_XPATH  + \
    EOP_NS   + "acquisitionParameters/" + \
    EOP_NS   + "Acquisition/" + \
    EOP_NS   + "incidenceAngle"

# <eop:product>
#   <eop:ProductInformation>
#     <eop:fileName>
#       <ows:ServiceReference xlink:href="http://some.url">
#         <ows:RequestMessage/>
#       </ows:ServiceReference>
#     </eop:fileName>
#   </eop:ProductInformation>
# </eop:product>

# <eop:mask>
#   <eop:MaskInformation>
#     <eop:type>CLOUD</eop:type>
#     <eop:format>RASTER</eop:format>
#     <eop:fileName>
#       <ows:ServiceReference xlink:href="http://some.url">
#         <ows:RequestMessage/>
#       </ows:ServiceReference>
#     </eop:fileName>
#   </eop:MaskInformation>
# </eop:mask>

EO_SERVICEREF_XPATH = EOP_NS + "fileName/" + OWS_NS + "ServiceReference"

# footprint:
#<eop:EarthObservation >
# <om:featureOfInterest>
#   <eop:Footprint gml:id="footprint_id">
#     <eop:multiExtentOf>
#       <gml:MultiSurface gml:id="multisurface_id" srsName="EPSG:4326">
#         <gml:surfaceMember>
#           <gml:Polygon gml:id="polygon_id">
#             <gml:exterior>
#               <gml:LinearRing>
#                 <gml:posList>
#                   42.835816 -1.005626 42.837949 -0.948104
#                   42.835816 -1.005626
#                 </gml:posList>
#
EO_MULTISURFACE_XPATH = \
    EO_EARTHOBSERVATION_XPATH + \
    OM_NS   + "featureOfInterest/" + \
    EOP_NS  + "Footprint/"    + \
    EOP_NS + "multiExtentOf/" + \
    GML_NS + "MultiSurface"

EO_POLYPOSLIST_XPATH = \
    GML_NS + "surfaceMember/" + \
    GML_NS + "Polygon/"  + \
    GML_NS + "exterior/" + \
    GML_NS + "LinearRing/" + \
    GML_NS + "posList/"
    

# ------------ XML metadata parsing --------------------------

def determine_wcs_type(caps):
    wcs_type = WCS_TYPE_UNKNOWN

    try:
        if hasattr(caps, 'ns_map'):
            for ns in caps.ns_map:
                uri = caps.ns_map[ns]
                if uri == CRS_URI_FINAL201:
                    wcs_type = WCS_TYPE_FINAL201
                    break
                elif uri == CRS_URI_DRAFT201:
                    wcs_type = WCS_TYPE_DRAFT201
                    break

    except Exception:
        caps_str = "<None>"
        if None != caps:
            caps_str = ET.tostring(caps)
        logger.error("determine_wcs_type() failed for: \n"+caps_str)
    
    return wcs_type

def get_coverageDescriptions(cd_tree):
    return cd_tree.findall("./" + \
                               WCS_NS + "CoverageDescriptions" + "/" +\
                               WCS_NS + "CoverageDescription")

def is_nc_tag(qtag, nctag): 
    parts=qtag.split("}")
    if len(parts)==1:
        return qtag==nctag
    elif len(parts)==2:
        return nctag==parts[1]
    else:
        return False

def tree_is_exception(tree):
    return is_nc_tag(tree.tag, EXCEPTION_TAG)


def extract_footprintpolys(cd):
    ms = cd.find("./" + EO_MULTISURFACE_XPATH)
    if not ms:
        logger.error("extract_footprintpoly: MultiSurface not found")
        return None
    try:
        srsName = ms.attrib['srsName']
    except KeyError:
        logger.error("extract_footprintpoly: srsName not found")
        return None

    coords = None
    ps = None
    lr = ms.findall("./" +
                 GML_NS + "surfaceMember/" +
                 GML_NS + "Polygon/" +
                 GML_NS + "exterior/" +
                 GML_NS + "LinearRing/")

    if not lr:
        logger.warning("extract_footprintpoly: LinearRing not found," +
                       " using bbox instead.")
        bb = extract_gml_bbox(cd)
        coords = [(bb.ll[0], bb.ll[1]), (bb.ur[0], bb.ur[1])]

    if len(lr) > 1:
        logger.warning("extract_footprintpoly: Multiple LinearRing found")

    for l in lr:
        ps = l.find("./"+GML_NS + "posList/")
        posList = ps.text
        coords = coords_from_text(posList, srsName)
        break  # support only one LR for now

    return coords


def add_missing_mediatype(href):
    #
    #  Adds media type if it is not in href already
    #
    #

    MEDIATYPE_STRING = "mediatype=multipart/mixed"

    if MEDIATYPE_STRING not in href:
        href += '&' +  MEDIATYPE_STRING

    return href

def extract_refs(eoresult, node_str, fix_missing_mtype=False):
    #
    #    node_str is either "product" or "mask"
    #
    # looking for either of:
    # //eop:product//eop:fileName/ows:ServiceReference[@xlink:href]
    #    //eop:mask//eop:fileName/ows:ServiceReference[@xlink:href]
    #
    #

    refs = []
    els = eoresult.findall(".//" + EOP_NS + node_str)
    for e in els:
        file_name_els = e.findall(".//" + EOP_NS +"fileName")
        for f in file_name_els:
            sr = f.find('./' + OWS_NS + "ServiceReference")
            if sr:
                href = sr.attrib.get(XLINK_NS+'href')
                if fix_missing_mtype:
                    href = add_missing_mediatype(href)
                refs.append(href)
    return refs

def extract_prods_and_masks(cd, fix_missing_mtype=False):
    prods_and_masks = []
    eoresults = cd.findall(".//" + OPT_NS   + "EarthObservationResult")
    for eor in eoresults:
        prods_and_masks.extend ( extract_refs(eor, "product", fix_missing_mtype) )
        prods_and_masks.extend ( extract_refs(eor, "mask",    fix_missing_mtype) )
    return prods_and_masks

def extract_path_text(cd, path):
    ret = None
    leaf_node = cd.find("./"+path)
    if None == leaf_node:
        return None
    return leaf_node.text


def extract_eoid(cd):
    return extract_path_text(cd, EO_IDENTIFIER_XPATH)


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
    bbox_to_WGS84(srsName, bb)
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
    # returns an instance of utils.TimePeriod
    tp = dss.find("./"+ GML_NS + "TimePeriod")
    if None == tp: return None
    begin_pos = tp.find("./"+ GML_NS + "beginPosition")
    end_pos   = tp.find("./"+ GML_NS + "endPosition")
    if None == begin_pos or None == end_pos: return None
    return TimePeriod(begin_pos.text, end_pos.text)

def extract_om_time(cd):
    phenomenonTime = cd.find("./" + EO_PHENOMENONTIME)
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

def extract_DatasetSeriesSummaries(caps):
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

def parse_with_ns(src_data):
    root = None
    events = "start", "start-ns"
    ns_map = []

    for event, elem in ET.iterparse(src_data, events):
        if event == "start-ns":
            ns_map.append(elem)
        elif event == "start":
            if root is None:
                root = elem
    root.ns_map = dict(ns_map)
    return root

def base_xml_parse(src_data, save_ns):
    root = None
    if save_ns:
        root = parse_with_ns(src_data)
    else:
        root = ET.parse(src_data).getroot()
    return root

def parse_file(src_data, expected_root, src_name, save_ns=False):
    result = None
    try:
        result = base_xml_parse(src_data, save_ns)
        if None == result:
            raise IngestionError("No data")
        if tree_is_exception(result):
            result = None
            logger.warning("'"+src_name+"' contains exception")
            if IE_DEBUG > 0:
                logger.info(ET.tostring(result))
        elif expected_root and not is_nc_tag(result.tag, expected_root):
            msg = "'"+src_name+"' does not contain expected root "+ \
                `expected_root` + ". In xml: "+`result.tag`
            logger.error(msg)
            result = None
        pass
    except IOError as e:
        loger.error("Cannot open/parse md source '"+src_name+"': " + `e`)
        return None
    except xml.parsers.expat.ExpatError as e:
        logger.error("Cannot parse '"+src_name+"', error="+`e`)
        return None
    except Exception as e:
        logger.error("Cannot parse '"+src_name+"', unknown error:" + `e`)
        return None

    return result

# ----------------- Stand-alone test/debug --------------------------
def set_logger(l):
    global logger
    logger = l
    print "ie_xml_parser.logger set to " + `logger`

if __name__ == '__main__':
    print "ie_xmlparser test"

    ex0_file = "/mnt/shared/WCS-change/wcsCapabilities.old.xml"
    ex1_file = "/mnt/shared/WCS-change/wcsCapabilities.new.xml"

    caps = parse_file(ex1_file,
                      "Capabilities",
                      "test_data: wcsCapabilities.new.xml",
                      True)

    if not caps:
        print "ERROR - no caps!"
    else:
        wcs_type = determine_wcs_type(caps)
        print "NEW wcs_type: " + `wcs_type`

    caps = None
    caps = parse_file(ex0_file,
                      "Capabilities",
                      "test_data: wcsCapabilities.old.xml",
                      True)
    if not caps:
        print "ERROR - no caps!"
    else:
        wcs_type = determine_wcs_type(caps)
        print "OLD wcs_type: " + `wcs_type`

    caps = None

    ex2_file = "/mnt/shared/Archive-2014-03-12/eox-caps-with-links.xml"
    cds = parse_file(ex2_file,
                     "CoverageDescriptions",
                     "test_data: eox-caps-with-links.xml")
    if not cds:
        print "ERROR - no cds!"
    else:
        p,m = extract_prods_and_masks(cds)
        print " prods: " + `p`
        print " masks: " + `m`
