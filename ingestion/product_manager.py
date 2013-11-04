############################################################
#  Project: DREAM
#  Module:  Task 5 ODA Ingestion Engine 
#  Authors:  Vojtech Stefka  (CVC), Milan Novacek (CVC)
#  Creation Date:  October 2013
#
#    (c) 2013 Siemens Convergence Creators s.r.o., Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
#  Product Manager module.
#
############################################################

from singleton_pattern import Singleton
import threading
import time
import logging
from settings import IE_DEBUG

@Singleton
class ProductManager(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        logger = logging.getLogger('dream.file_logger')
        if IE_DEBUG > 1:
            logger.debug('Product Manager init.',extra={'user':"drtest"})
    
    def run(self):
        logger = logging.getLogger('dream.file_logger')
        while True:
            if IE_DEBUG > 1:
                logger.debug("Product Manager is running.",
                             extra={'user':"drtest"})
            time.sleep(15) 
