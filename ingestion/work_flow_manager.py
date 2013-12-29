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
import traceback
import subprocess
import sys

import models
import views
from django.utils.timezone import utc
from settings import IE_DEBUG
from ingestion_logic import ingestion_logic
from add_product import add_product_wfunc
from utils import UnsupportedBboxError, IngestionError, create_manifest

worker_id = 0

#**************************************************
#                 Work Task                       *
#**************************************************
class WorkerTask:
    def __init__(self,parameters):
        # parameters must contain at least 'task_type'.
        # task_types are the keys for Worker.task_functions, see below.
        # Different task types then have their specific parameters.
        # E.g. for the INGEST_SCENARIO, these are:
        #  {
        #     "task_type",
        #     "scenario_id",
        #     "scripts"
        #   }
        #
        self._parameters = parameters
        ss = WorkFlowManager.Instance().set_scenario_status
        if "scenario_id" in parameters:
            # set percent done to at least 1% to keep the web page updates active
            ss(0, parameters["scenario_id"], 0, "QUEUED", 1)

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
        self.task_functions = {
            "DELETE_SCENARIO": self.delete_func,
            "INGEST_SCENARIO": self.ingest_func,
            "ADD_PRODUCT":     add_product_wfunc       # from add_product
            }

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
            if queue.empty():
                time.sleep(1)

    def do_task(self,current_task):
        # set scenario status (processing)
        parameters = current_task._parameters
        task_type = parameters['task_type']
        if IE_DEBUG > 1:
            self._logger.debug( "do_task: "+task_type )

        try:
            self.task_functions[task_type](parameters)
        except KeyError:
            self._logger.warning(
                "There is no type of the current task to process. (" + \
                    task_type + ")", extra={'user':"drtest"} )
        except Exception as e:
            self._logger.error(
                "Worker do_task caught exception " + `e` +
                "- Recovering from Internal Error")

    def delete_func(self,parameters):
            self._wfm.set_scenario_status(
                self._id,parameters["scenario_id"],0,"DELETING",1)

            # TODO XXX TMP FOR DEVELOPMENT DELETE FOR PRODUCTION XXX
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

    def ingest_func(self,parameters):
        if IE_DEBUG > 0:
            self._logger.info(
                "wfm: executing INGEST_SCENARIO, id=" +\
                    `parameters["scenario_id"]`)

        percent = 1
        sc_id = parameters["scenario_id"]
        self._wfm.set_scenario_status(
            self._id, sc_id, 0, "GENERATING URLS", percent)
        try:
            scenario = models.Scenario.objects.get(id=sc_id)

            eoids = scenario.eoid_set.all()
            eoid_strs = []
            for e in eoids:
                eoid_strs.append(e.eoid_val)

            #extraconditions = scenario.extraconditions_set.all()
            extras_list = []
            #for e in extraconditions:
            #    extras_list.append( (e.xpath, e.value) )

            # ingestion_logic blocks until DM is finished downloading
            dl_dir, dar_url, dar_id = \
                ingestion_logic(sc_id,
                                models.scenario_dict(scenario),
                                eoid_strs,
                                extras_list)

            if None == dar_id:
                raise IngestionError("No DAR generated")

            # For each product that was downloaded into its seperate
            # directory, generate a product manifest for the ODA server,
            # and also split each downloaded product into its parts.
            # Then run the ODA ingestion script.
            # TODO: the splitting could be done by the EO-WCS DM plugin
            #       instead of doing it here
            dir_list = os.listdir(dl_dir)
            n_dirs = len(dir_list)
            scripts = parameters["scripts"]
            nerrors = 0
            i = 1
            for d in dir_list:
                mf_name = create_manifest(
                    os.path.join(dl_dir, d), scenario.ncn_id, self._logger)
                if not mf_name:
                    nerrors += 1
                    continue

                # run ingestion scripts
                for script in scripts: # scripts absolute path
                    self._logger.info("Running script: %s" % script)
                    r = subprocess.call([script, mf_name])
                    if 0 != r:
                        nerrors += 1
                        self._logger.error("Ingest script returned status:"+`r`)
                    percent  = 100 * (float(i) / float(n_dirs))
                    # keep percent > 0 to ensure webpage updates
                    if percent < 1.0: percent = 1
                    self._wfm.set_scenario_status(
                        self._id, sc_id, 0, "INGESTING", percent)
                i += 1

            if nerrors>0:
                raise IngestionError("Ingestion encountered "+ `nerrors` +" errors")

            # Finished
            self._wfm.set_scenario_status(self._id, sc_id, 1, "IDLE", 0)
            self._logger.info("Ingestion completed.")

        except Exception as e:
            self._logger.error("Error while ingesting: " + `e`)
            self._wfm.set_scenario_status(self._id, sc_id, 1, "INGEST ERROR", 0)
            if IE_DEBUG > 0:
                traceback.print_exc(12,sys.stdout)


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
                        % (scenario.id, scenario.starting_date,
                           scenario.repeat_interval))
                # use the following for tz-aware datetimes
                #t_now = datetime.datetime.utcnow().replace(tzinfo=utc)
                t_now = datetime.datetime.utcnow()
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
             self._logger.error ("Current_task is not a task.")


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


