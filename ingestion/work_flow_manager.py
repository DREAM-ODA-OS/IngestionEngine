############################################################
#  Project: DREAM
#  Module:  Task 5 ODA Ingestion Engine 
#  Author:  Vojtech Stefka  (CVC)
#  Contribution: Milan Novacek (CVC)
#
#    (c) 2013 Siemens Convergence Creators s.r.o., Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
#  Ingestion Engine Workflow manager.
#
############################################################

from singleton_pattern import Singleton
import threading
import logging
import time
import Queue
import os
import shutil
import datetime

import models
import views
from django.utils.timezone import utc
from settings import IE_DEBUG
from ingestion_logic import ingestion_logic
from utils import scenario_dict, UnsupportedBboxError

worker_id = 0

#**************************************************
#                 Work Task                       *
#**************************************************
class WorkerTask:
    def __init__(self,parameters):
        # parameters are:
        #  {
        #     "task_type":"",  # DELETE_SCENARIO, INGEST_SCENARIO
        #     "scripts":[]
        #   }
        self._parameters = parameters

#**************************************************
#                 Worker                          *
#**************************************************
class Worker(threading.Thread):
    def __init__(self,work_flow_manager):
        threading.Thread.__init__(self)
        global worker_id
        self._id  = worker_id
        worker_id = worker_id + 1
        self._wfm = work_flow_manager
        self._logger = logging.getLogger('dream.file_logger')

    def run(self):
        if IE_DEBUG > 3:
            self._logger.debug(
                "Worker-%d of Work-Flow Manager is running." % self._id,
                extra={'user':"drtest"}  )
        while True:
            queue = self._wfm._queue
            current_task = queue.get()
            self.do_task(current_task)
            queue.task_done()
            time.sleep(1)

    def do_task(self,current_task):
        # set scenario status (processing)
        parameters = current_task._parameters
        task_type = parameters['task_type']
        if IE_DEBUG > 1:
            self._logger.debug( "do_task: "+task_type, extra={'user':"drtest"} )
        if task_type=="DELETE_SCENARIO":
            self._wfm.set_scenario_status(
                self._id,parameters["scenario_id"],0,"DELETING",0)

            # TODO XXX TMP FOR DEVELOPMENT DELETE FOR PRODUCITON XXX
            time.sleep(2)
            self._wfm.set_scenario_status(
                self._id,parameters["scenario_id"],0,"DELETING",50)
            time.sleep(2)

            # TODO XXX TMP FIX THIS
            # do deleting scenario
            for script in parameters["scripts"]: # scripts absolute path
                os.system(script)
            self._wfm.set_scenario_status(
                self._id,parameters["scenario_id"],0,"DELETING",100)

            # delete scenario from the db using django commands
            #scenario_id = parameters["scenario_id"]
            #scenario = models.Scenario.objects.get(id=int(scenario_id))
            #scripts = scenario.script_set.all()
            #views.delete_scripts(scripts)
            #scenario.delete()

            # TODO XXX END OF TMP / FIX THIS XXX

            # jquery should not count on this scenario any more
            self._wfm.set_scenario_status(
                self._id,parameters["scenario_id"],0,"DELETED",100) 

        elif task_type=="INGEST_SCENARIO":
            if IE_DEBUG > 0:
                self._logger.info(
                    "wfm: executing INGEST_SCENARIO, id=" +\
                        `parameters["scenario_id"]`)
            #self._wfm.set_scenario_status(
            #    self._id,parameters["scenario_id"],0,"INGESTING",0)

            percent = 0.0
            sc_id = parameters["scenario_id"]
            self._wfm.set_scenario_status(
                self._id, sc_id, 0, "GENERATING URLS", percent)
            try:
                scenario_data = models.Scenario.objects.get(id=sc_id)
                dl_dir = ingestion_logic(scenario_dict(scenario_data))

                # run ingestion scripts
                scripts = parameters["scripts"]
                for script in scripts: # scripts absolute path
                    self._logger.info("Running script: %s" % script)
                    os.system(script)  # TODO use subprocess.Popen
                    percent += (100.0/float(len(scripts)))
                    self._wfm.set_scenario_status(
                        self._id, sc_id, 0, "INGESTING", percent)
                self._logger.info(" ****** TODO: ****** NOT Removing "+dl_dir)
                # TODO remove tmp download dir after ingestion is complete
                print "          Tmp dowload dir should be removed in production"
                #shutil.rmtree(dl_dir)

            except Exception as e:
                self._logger.error("Error while ingesting: " + `e`)
                self._wfm.set_scenario_status(self._id, sc_id, 1, "INGEST ERROR", 0)

            self._wfm.set_scenario_status(self._id, sc_id, 1, "IDLE", 0)
            self._logger.info("Ingestion completed.")

        elif task_type=="ADD-PRODUCT":
            # parameters: addProduct_id,dataRef,addProductScript
            # run addProduct script
            addProduct = models.ProductInfo.objects.filter(
                id=parameters["addProduct_id"])[0]
            addProduct.info_status = "processing"
            try:
                command = "%s %s" % (
                    parameters["addProductScript"],
                    parameters["dataRef"])
                self._logger.info( "Running - %s" % command )
                
                os.system(command) # TODO use subprocess.Popen
                addProduct.info_status = "done"
            except Exception as e:
                addProduct.info_error = "Error %s" % e
                addProduct.info_status = "failed"
            finally:
                addProduct.save()
        else:
            self._logger.warning(
                "There is no type of the current task to process. (" + \
                    task_type + ")", extra={'user':"drtest"} )
        # can be extended

#**************************************************
#      Auto-Ingest-Scenario-Worker                *
#**************************************************
class AISWorker(threading.Thread):
    def __init__(self,work_flow_manager):
        threading.Thread.__init__(self)
        global worker_id
        self._id = worker_id
        worker_id = worker_id + 1
        self._wfm = work_flow_manager
        self._logger = logging.getLogger('dream.file_logger')

    def run(self): # manages auto-ingestion of scenario
        while True:
            # read all scenarios
            if IE_DEBUG > 3: self._logger.debug(
                "AISWorker-%d of Work-Flow Manager is running." % self._id, \
                extra={'user':"drtest"})
            scenarios = models.Scenario.objects.all()
            for scenario in scenarios:
                if IE_DEBUG > 3: self._logger.debug (
                    "Scenario: %d starting_date: %s  repeat_interval: %d" \
                        % (scenario.id,scenario.starting_date,
                           scenario.repeat_interval))
                t_now = datetime.datetime.utcnow().replace(tzinfo=utc)
                if scenario.starting_date <= t_now and scenario.repeat_interval!=0:
                    t_delta = datetime.timedelta(seconds=scenario.repeat_interval)
                    t_prev = t_now - t_delta
                    if scenario.starting_date <= t_prev:
                        scenario.starting_date = t_prev
                    while scenario.starting_date <= t_now:
                        scenario.starting_date += t_delta
                        if IE_DEBUG > 2: self._logger.debug (
                            "Scenario %d - new time: %s" % \
                                (scenario.id,scenario.starting_date),
                            extra={'user':"drtest"})
                    # put task to queue to process
                    ingest_scripts = []
                    scripts = scenario.script_set.all()
                    for s in scripts:
                        ingest_scripts.append("%s" % s.script_path)
                    current_task =  WorkerTask(
                        {"scenario_id":scenario.id,
                         "task_type":"INGEST_SCENARIO",
                         "scripts":ingest_scripts})
                    scenario.save() # save updated starting_date
                    self._wfm.put_task_to_queue(current_task)
            time.sleep(60) # repeat checking every 1 minute


#**************************************************
#               Work-Flow-Manager                 *
#**************************************************
@Singleton
class WorkFlowManager:
    def __init__(self):
        self._queue = Queue.LifoQueue() # first items added are first retrieved
        self._workers = []
        self._workers.append(Worker(self))
        self._workers.append(Worker(self))
        self._workers.append(AISWorker(self))
        self._lock_db = threading.Lock()
        self._logger = logging.getLogger('dream.file_logger')


    def put_task_to_queue(self,current_task):
        if isinstance(current_task,WorkerTask):
            self._queue.put(current_task)
        else:
             self._logger.error ("Current_task is not task.")


    def set_scenario_status(
        self,
        worker_id,
        scenario_id,
        is_available,
        status,
        done):
        self._lock_db.acquire()
        if IE_DEBUG > 3:
            self._logger.debug( "Worker-%d uses db." % worker_id)
        try:
            # set scenario status
            scenario_status = models.ScenarioStatus.objects.get(
                scenario_id=scenario_id)
            scenario_status.is_available = is_available
            scenario_status.status = status
            scenario_status.done = done
            scenario_status.save()
        except Exception as e:
            self._logger.error(`e`)
        finally:
            self._lock_db.release()
            if IE_DEBUG > 3:
                self._logger.debug( "Worker-%d stops using db." % worker_id)


    def delete_scenario(self):
        # delete scenario, scripts and scenario-status from db print
        #  (scenario-status is bounded to scenario)
        pass

    def start(self):
        for w in self._workers:
            w.setDaemon(True)
            w.start()


