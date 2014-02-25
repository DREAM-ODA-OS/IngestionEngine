############################################################
#  Project: DREAM
#
#  Module:  Task 5 ODA Ingestion Engine 
#
#  Author: Milan Novacek (CVC)
#
#  Date:   Jan 9, 2014
#
#    (c) 2014 Siemens Convergence Creators s.r.o., Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
#  Ingestion Engine: darc: Data ARChive
#   Stores in the database associated with a scenario the coverage_id
#    of downloaded products to be able to check later whether that
#    data has aleady been downloaded, to avoid downloading it again.
#
#  Proceeds by reading in the metada xml from the file on disc,
#  then adds a record in the database Archive table.
#
############################################################

import logging

from settings import IE_DEBUG

from models import \
    Scenario, \
    Archive

from ie_xml_parser import \
    base_xml_parse, \
    extract_eoid

logger = logging.getLogger('dream.file_logger')

def archive_metadata(sc_id, metafile):
    # cd_tree: coverage description tree extracted from the
    #          metadata XML file
    cd_tree = base_xml_parse(metafile)

    coverage_id = extract_eoid(cd_tree)

    if IE_DEBUG > 1:
        logger.info("Sc_id " + `sc_id` + ": Archiving meta for " + `cd_tree.tag`+
                    ", cid='"+coverage_id+"'.")

    cd_tree = None

    scenario = Scenario.objects.get(id=int(sc_id))

    archive_record = Archive(
        scenario = scenario,
        eoid     = coverage_id)

    archive_record.save()

    return True

