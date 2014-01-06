############################################################
#  Project: DREAM
#  Module:  Task 5 ODA Ingestion Engine 
#  Contribution: Milan Novacek (CVC)
#  Date:    Oct 20, 2013
#
#    (c) 2013 Siemens Convergence Creators s.r.o., Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
#  Ingestion Engine: Data Access Request (DAR) builder.
#
############################################################

import xml.etree.ElementTree as ET
import sys

LOCAL_DEBUG = 0

DAR_PREAMBLE = '<?xml version="1.0" encoding="UTF-8"?>'
TODO = """<ngeo:DataAccessMonitoring-Resp
    xmlns:ngeo="http://ngeo.eo.esa.int/iicd-d-ws/1.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://ngeo.eo.esa.int/iicd-d-ws/1.0 IF-ngEO-DataAccessMonitoring-Resp.xsd">
    <ngeo:MonitoringStatus>IN_PROGRESS</ngeo:MonitoringStatus>"""

NGEO_URI = "http://ngeo.eo.esa.int/iicd-d-ws/1.0"
NGEO_NS  = "{"+NGEO_URI+"}"
XSI_URI  = "http://www.w3.org/2001/XMLSchema-instance"
XSI_NS   = "{"+XSI_URI+"}"
SCHEMA_LOCATION = "http://ngeo.eo.esa.int/iicd-d-ws/1.0 IF-ngEO-DataAccessMonitoring-Resp.xsd"
DA_RESP = "DataAccessMonitoring-Resp"
MONITORINGSTATUS = "MonitoringStatus"
PRODACCESSLIST = "ProductAccessList"
PRODACCESS = "ProductAccess"
PRODACCESSURL = "ProductAccessURL"
PRODACCESSSTATUS = "ProductAccessStatus"
PRODDOWNLOADDIRECTORY = "ProductDownloadDirectory"

use_register = False
vers = sys.version_info
if vers[0] > 2: use_register = True
if vers[1] > 6 and vers[0] == 2: use_register = True

def build_DAR(urls):
    """ urls consist of a list of tuples (dl_dir, url),
    where dl_dir is the download directory for each url"""

    if use_register:
        try:
            ET.register_namespace("ngeo", NGEO_URI)
            ET.register_namespace("xsi",  XSI_URI)
        except Exception:
            pass

    root = ET.Element(NGEO_NS+DA_RESP,
                      {XSI_NS+"schemaLocation":SCHEMA_LOCATION})
    ET.SubElement(root, NGEO_NS+MONITORINGSTATUS).text = "IN_PROGRESS"
    pa_list = ET.SubElement(root, NGEO_NS+PRODACCESSLIST)

    for url in urls:
        pa = ET.SubElement(pa_list, NGEO_NS+PRODACCESS)
        ET.SubElement(pa, NGEO_NS+PRODACCESSURL).text = url[1]
        ET.SubElement(pa, NGEO_NS+PRODACCESSSTATUS).text = "READY"
        ET.SubElement(pa, NGEO_NS+PRODDOWNLOADDIRECTORY).text =  url[0]

    if LOCAL_DEBUG > 0:
        print "dAR:\n"+ DAR_PREAMBLE + ET.tostring(root)

    return DAR_PREAMBLE + ET.tostring(root)
