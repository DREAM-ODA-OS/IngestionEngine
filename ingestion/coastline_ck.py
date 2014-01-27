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
#  Ingestion Engine: Perform Coastline Check
#   checks if the area described in the metadata is within
#   the coastline polygon
#
#  used by ingestion_logic
#
############################################################

import logging
import json
import os.path
import traceback

from settings import \
    IE_DEBUG

from utils import mkFname

logger = logging.getLogger('dream.file_logger')

def coastline_ck(coverageDescription):
    if IE_DEBUG > 0:
        logger.info('  performing coastline_check')
    logger.error(' coastline_check is not implemented')

    return True
