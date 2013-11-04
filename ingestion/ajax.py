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
import settings
import forms
import views
import work_flow_manager
import logging
import os


@dajaxice_register(method='POST')
def delete_scenario_wfm(request,scenario_id):
    logger = logging.getLogger('dream.file_logger')
    print "Delete scenario WFM"
    # it runs deleting scripts
    scenario = models.Scenario.objects.get(id=int(scenario_id))
    scripts = scenario.script_set.all()
    #for s in scripts:
    #    print s.script_name
    # send request/task to work-flow-manager to run delete script
    wfm = work_flow_manager.WorkFlowManager.Instance()
    user = request.user

    del_scripts = models.UserScript.objects.filter(
        script_name__startswith="deleteScenario-",user_id__exact=user.id)
    if len(del_scripts)>0:
        del_script = del_scripts[0]
        current_task = work_flow_manager.WorkerTask(
            {"scenario_id":scenario_id,
             "task_type":"DELETING-SCENARIO",
             "scripts":["%s/%s" % (settings.MEDIA_ROOT,del_script.script_file)]})
        wfm.put_task_to_queue(current_task)
    else:
       # use logging
       logger = logging.getLogger('dream.file_logger')
       logger.warning(
           'Scenario: id=%d name=%s does not have a delete script.' \
               % (scenario.id,scenario.scenario_name),
           extra={'user':request.user})
    return simplejson.dumps({})


@dajaxice_register(method='GET')
def synchronize_scenarios(request):
    logger = logging.getLogger('dream.file_logger')
    print "Synchronize scenario"
    user = request.user
    scenarios = user.scenario_set.all()
    results = []
    i = 0
    for scenario in scenarios:
        ss = scenario.scenariostatus
        result = [scenario.id,
                  scenario.repeat_interval,
                  ss.id,
                  ss.is_available,
                  ss.status,
                  ss.done]
        results.append(result)
        i = i + 1
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
def read_logging(request,message_type):
    user = request.user
    messages = []
    for file_name in os.listdir(settings.LOGGING_DIR):
        file_path = os.path.join(settings.LOGGING_DIR,file_name)
        f = open(file_path,'r')
        for line in f:
            if line.split(" ")[4]==user.username:
                #if line.split(" ")[0]==message_type and line.split(" ")[4]==user.username:
                messages.append(line)         # doTo: sort messages by date !!!
        f.close()
    return simplejson.dumps({'message':messages})

@dajaxice_register(method='POST')
def ingest_scenario_wfm(request,scenario_id):
    # ingest scenario - run all scripts related to the scenario
    # it runs deleting scripts
    logger = logging.getLogger('dream.file_logger')
    scenario = models.Scenario.objects.get(id=int(scenario_id))
    scripts = scenario.script_set.all()
    ingested_scripts = []
    for s in scripts:
        ingested_scripts.append("%s" % s.script_path)
    # send request/task to work-flow-manager to run delete script
    wfm = work_flow_manager.WorkFlowManager.Instance()
    user = request.user

    if len(ingested_scripts)>0:
        current_task = work_flow_manager.WorkerTask(
            {"scenario_id":scenario_id,
             "task_type":"INGESTING-SCENARIO",
             "scripts":ingested_scripts})
        wfm.put_task_to_queue(current_task)
        logger.info(
            'Operation: ingest scenario: id=%d name=%s' \
                % (scenario.id,scenario.scenario_name),
            extra={'user':request.user})
    else:
        logger.warning(
            'Scenario: id=%d name=%s does not have scripts to ingest.' \
                % (scenario.id,scenario.scenario_name),
            extra={'user':request.user})
    return simplejson.dump({})









































    
    
