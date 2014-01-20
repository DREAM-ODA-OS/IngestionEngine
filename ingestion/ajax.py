############################################################
#  Project: DREAM
#  Module:  Task 5 ODA Ingestion Engine 
#  Author:  Vojtech Stefka  (CVC)
#
#    (c) 2013 Siemens Convergence Creators s.r.o., Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
#  Ajax functions.
#
############################################################

import simplejson
from dajax.core import Dajax

from dajaxice.utils import deserialize_form
from dajaxice.decorators import dajaxice_register

import models
import forms
import views
import work_flow_manager
import logging
import os

from settings import \
    MEDIA_ROOT, \
    LOGGING_FILE, \
    BROWSER_N_LOGLINES

from dm_control import DownloadManagerController

dmcontroller = DownloadManagerController.Instance()

@dajaxice_register(method='POST')
def delete_scenario_wfm(request, ncn_id):
    return simplejson.dumps(
        views.delete_scenario_core(ncn_id=ncn_id))

@dajaxice_register(method='GET')
def test1(request):
    user = request.user
    scenarios = user.scenario_set.all()
    results = []
    for scenario in scenarios:
        ss = scenario.scenariostatus
        result = [scenario.id,
                  scenario.ncn_id,
                  scenario.repeat_interval,
                  ss.id,
                  0,
                  ss.status,
                  ss.done]
        results.append(result)
    return simplejson.dumps({'jscenario_status':results})
    
@dajaxice_register(method='GET')
def synchronize_scenarios(request, scenario_id=None):
    logger = logging.getLogger('dream.file_logger')
    user = request.user
    scenarios = user.scenario_set.all()
    results = []
    for scenario in scenarios:
        ss = scenario.scenariostatus
        result = [scenario.id,
                  scenario.ncn_id,
                  scenario.repeat_interval,
                  ss.id,
                  ss.is_available,
                  ss.status,
                  ss.done]
        results.append(result)
    return simplejson.dumps(
        {'jscenario_status':results,
         'op_sc':scenario_id})


@dajaxice_register(method='POST')
def read_logging(request, message_type, max_log_lines):
    messages = []
    if max_log_lines == '':
        maxll = BROWSER_N_LOGLINES
    else:
        maxll = int(max_log_lines)
    mtype = message_type.encode('ascii','ignore')
    try:
        f = open(LOGGING_FILE,'r')
        for line in f:
            lstr = line.strip()
            parts = lstr.split(' ',3)
            messages.append(parts)
        f.close()
        if len(messages) > maxll:
            messages = messages[-maxll:]
        return simplejson.dumps({'message':messages})
    except Exception as e:
        logging.getLogger('dream.file_logger').error(
            "read_logging error: " + `e`)
        return simplejson.dumps(
            {'message':[['','','Error accessing the log file.','']]})

@dajaxice_register(method='POST')
def ingest_scenario_wfm(request, ncn_id):
    ncn_id = ncn_id.encode('ascii','ignore')
    # make sure the IE port is set-up
    server_port = request.META['SERVER_PORT']
    dmcontroller.set_condIEport(server_port)
    try:
        return simplejson.dumps(
            views.ingest_scenario_core(ncn_id=ncn_id)
            )
    except Exception as e:
        return simplejson.dumps({'error': `e`})
        

@dajaxice_register(method='POST')
def stop_ingestion_wfm(request,scenario_id):
    logger = logging.getLogger('dream.file_logger')
    logger.info("stop_ingestion_wfm, id="+`scenario_id`)
    views.stop_ingestion_core(scenario_id)
    return simplejson.dumps({})

