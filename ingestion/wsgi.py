############################################################
#  Project: DREAM
#  Module:  Task 5 ODA Ingestion Engine 
#  Author:  Vojtech Stefka  (CVC)
#
#    (c) 2013 Siemens Convergence Creators s.r.o., Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
# WSGI config for dream project.
#
#
############################################################

'''
This module contains the WSGI application used by Django's development 
server and any production WSGI deployments. It should expose a 
module-level variable named ``application``. Django's ``runserver`` and 
``runfcgi`` commands discover this application via the ``WSGI_APPLICATION``
setting.

Usually you will have the standard Django WSGI application here, but it 
also might make sense to replace the whole Django WSGI application with a
custom one that later delegates to the Django one. For example, you could
introduce WSGI middleware here, or combine a Django application with an
application of another framework.

'''

import os
import logging

import dm_control
import product_manager
import work_flow_manager
from settings import IE_PROJECT
    
# We defer to a DJANGO_SETTINGS_MODULE already in the environment.
# This breaks if running multiple sites in the same mod_wsgi process. 
# To fix this, use mod_wsgi daemon mode with each site in its own 
# daemon process, or use
# os.environ["DJANGO_SETTINGS_MODULE"] = "<project>.settings"
os.environ.setdefault("DJANGO_SETTINGS_MODULE", IE_PROJECT + ".settings")

# This application object is used by any WSGI server configured to use this
# file. This includes Django's development server, if the WSGI_APPLICATION
# setting points here.
from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()

#  Misc. Initialisations
logger = logging.getLogger('dream.file_logger')

# start the ngEO download manager (external process)
dmcontroller = dm_control.DownloadManagerController.Instance()
print `dmcontroller`
dm_is_running = dmcontroller.start_dm()
if not dm_is_running:
    logger.warning("Failed to start Download Manager, "+
                   "proceeding with Ingestion Engine start-up regardless.")

# start work-flow manager
wfmanager = work_flow_manager.WorkFlowManager.Instance()
wfmanager.start()

# start product manager
pmanager = product_manager.ProductManager.Instance()
pmanager.setDaemon(True)
#pmanager.start() 
