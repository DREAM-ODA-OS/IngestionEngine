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
    BROWSER_N_LOGLINES, \
    IE_SCRIPTS_DIR, \
    IE_DEFAULT_INGEST_SCRIPT, \
    IE_DEFAULT_DEL_SCRIPT

from dm_control import DownloadManagerController

dmcontroller = DownloadManagerController.Instance()

@dajaxice_register(method='POST')
def delete_scenario_wfm(request,scenario_id):
    ret = {}
    logger = logging.getLogger('dream.file_logger')
    # it runs deleting scripts
    scenario = models.Scenario.objects.get(id=int(scenario_id))
    scripts = scenario.script_set.all()
    ncn_id = scenario.ncn_id
    #for s in scripts:
    #    print s.script_name
    # send request/task to work-flow-manager to run delete script
    wfm = work_flow_manager.WorkFlowManager.Instance()

    if not wfm.lock_scenario(scenario_id):
        msg = "Scenario '%s' name=%s is busy." % (scenario.ncn_id, scenario.scenario_name)
        logger.warning("Delete Scenario refused: " + msg)
        ret = {'status':1, 'message':"Error: "+msg}
    else:

        logger.info ("Deleting scenario ncn_id="+`ncn_id`)
        del_scripts = []
        default_delete_script = os.path.join(
            IE_SCRIPTS_DIR, IE_DEFAULT_DEL_SCRIPT)
        #del_scripts = models.UserScript.objects.filter(
        #    script_name__startswith="deleteScenario-",user_id__exact=user.id)
        if len(del_scripts)>0:
            del_script = del_scripts[0]
            current_task = work_flow_manager.WorkerTask(
                {"scenario_id":scenario_id,
                 "task_type":"DELETE_SCENARIO",
                 "scripts":["%s/%s" % (MEDIA_ROOT,del_script.script_file)]})
            wfm.put_task_to_queue(current_task)
        
        elif default_delete_script:
            current_task = work_flow_manager.WorkerTask(
                {"scenario_id":scenario_id,
                 "task_type":"DELETE_SCENARIO",
                 "scripts":[default_delete_script]
                 })
            wfm.put_task_to_queue(current_task)
        
        else:
           logger.warning(
               'Scenario: id=%d name=%s does not have a delete script.' \
                   % (scenario.id,scenario.scenario_name),
               extra={'user':request.user})
        ret = {'status':0,}

    return simplejson.dumps(ret)


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
def synchronize_scenarios(request):
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
    return simplejson.dumps({'jscenario_status':results})


@dajaxice_register(method='POST')
def delete_scenario_django(request,scenario_id):
    # delete scenario from the db using django commands
    logger = logging.getLogger('dream.file_logger')
    print "Delete scenario DJANGO"
    scenario = models.Scenario.objects.get(id=int(scenario_id))
    scripts = scenario.script_set.all()
    views.delete_scripts(scripts)

    logger.info(
        'Operation: delete scenario: id=%d name=%s' \
            % (scenario.id,scenario.scenario_name),
        extra={'user':request.user})

    scenario.delete()
    return simplejson.dumps(
        {'message':"Scenario %d is deleted." % int(scenario_id)})

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
def ingest_scenario_wfm(request,scenario_id):
    ret = {}
    # ingest scenario - run all ing. scripts related to the scenario
    logger = logging.getLogger('dream.file_logger')
    # make sure the IE port is set-up
    pp = request.META['SERVER_PORT']
    dmcontroller.set_condIEport(pp)
    scenario = models.Scenario.objects.get(id=int(scenario_id))
    wfm = work_flow_manager.WorkFlowManager.Instance()
    if not wfm.lock_scenario(scenario_id):
        msg = "Scenario '%s' name=%s is busy." % (scenario.ncn_id, scenario.scenario_name)
        logger.warning("Ingest Scenario refused: " + msg)
        ret = {'status':1, 'message':"Error: "+msg}
    else:

        scripts = models.get_scenario_script_paths(scenario)
        if len(scripts) > 0:
            # send request/task to work-flow-manager to run script
            current_task = work_flow_manager.WorkerTask(
                {"scenario_id":scenario_id,
                 "task_type":"INGEST_SCENARIO",
                 "scripts":scripts})

            wfm.put_task_to_queue(current_task)
            logger.info(
                'Operation: ingest scenario: id=%d name=%s' \
                    % (scenario.id,scenario.scenario_name))
            ret = {'status':0,
                   "message":"Ingestion Submitted to processing queue."}
        else:
            msg = "Scenario '%s' name=%s does not have scripts to ingest." \
                    % (scenario.ncn_id,scenario.scenario_name)
            logger.warning(msg)
            ret = {'status':1, 'message':"Error"+msg}

    return simplejson.dumps(ret)


@dajaxice_register(method='POST')
def stop_ingestion_wfm(request,scenario_id):
    # TODO actually stop an ongoing ingestion if any
    logger = logging.getLogger('dream.file_logger')
    logger.info("stop_ingestion_wfm, id="+`scenario_id`)
    wfm = work_flow_manager.WorkFlowManager.Instance()
    wfm.set_stop_request(scenario_id)
    return simplejson.dumps({})

