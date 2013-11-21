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

INDENT_STRING = '    '
DAR_PREAMBLE = """<?xml version="1.0" encoding="UTF-8"?>
<ngeo:DataAccessMonitoring-Resp
    xmlns:ngeo="http://ngeo.eo.esa.int/iicd-d-ws/1.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://ngeo.eo.esa.int/iicd-d-ws/1.0 IF-ngEO-DataAccessMonitoring-Resp.xsd">
    <ngeo:MonitoringStatus>IN_PROGRESS</ngeo:MonitoringStatus>"""

DAR_TAIL = """</ngeo:DataAccessMonitoring-Resp>
"""

NGEO_NS = "ngeo"
PRODACCESSLIST = "ProductAccessList"
PRODACCESS = "ProductAccess"
PRODACCESSURL = "ProductAccessURL"
PRODACCESSSTATUS = "ProductAccessStatus"
PRODDOWNLOADDIRECTORY = "ProductDownloadDirectory"
XML_INDENT = 4*' '

def element(prefix, tag, contents):
    return "<%s:%s>%s</%s:%s>" % \
        (prefix,tag,contents,prefix,tag)

def join_and_indent(indent, lines, add_newlines=True):
    tmp_str = "\n".join(lines)
    nl = ''
    if add_newlines: nl = '\n'
    return nl+indent+tmp_str.replace("\n", "\n" + indent)+nl

def build_DAR(urls):
    """ urls consist of a list of tuples (dl_dir, url),
    where dl_dir is the download directory for each url"""
    dar_products = []
    for url in urls:
        elems = [
            element(NGEO_NS, PRODACCESSURL,         url[1]),
            element(NGEO_NS, PRODACCESSSTATUS,     "READY"),
            element(NGEO_NS, PRODDOWNLOADDIRECTORY, url[0])
            ]
        prod_access = join_and_indent(XML_INDENT, elems)
        dar_products.append(element(NGEO_NS, PRODACCESS, prod_access))
    indented_products = join_and_indent(XML_INDENT, dar_products)
    dar_ProductAccessList = [element(NGEO_NS, PRODACCESSLIST, indented_products),]
    
    return \
        DAR_PREAMBLE + \
        join_and_indent(XML_INDENT,dar_ProductAccessList) + \
        DAR_TAIL
