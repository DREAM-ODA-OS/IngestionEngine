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
import Queue
import os
import shutil
import time
import datetime
import calendar
import traceback
import subprocess
import sys

import models

from views import \
    delete_scripts, \
    submit_scenario

from django.utils.timezone import utc

from settings import \
    IE_DEBUG, \
    IE_N_WORKFLOW_WORKERS, \
    STOP_REQUEST, \
    IE_BEAM_HOME, \
    IE_SCRIPTS_DIR, \
    IE_DEFAULT_CATREG_SCRIPT, \
    IE_DEFAULT_CATDEREG_SCRIPT, \
    IE_S2ATM_PREPROCESS_SCRIPT, \
    IE_DIMAPMETA_SUFFIX, \
    IE_S2ATM_OUT_SUFFIX, \
    IE_TAR_RESULT_SCRIPT, \
    IE_TAR_FILE_SUFFIX

from ingestion_logic import \
    ingestion_logic, \
    check_status_stopping, \
    stop_active_dar_dl

from darc import archive_metadata

from add_product import add_product_wfunc

from utils import \
    UnsupportedBboxError, \
    IngestionError, \
    StopRequest, \
    create_manifest, \
    split_and_create_mf, \
    ie_unpack_maybe, \
    get_glob_list, \
    extract_outfile

worker_id = 0

#**************************************************
#                 Work Task                       *
#**************************************************
class WorkerTask:
    def __init__(self, parameters, set_status=True):
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
        if set_status and "scenario_id" in parameters:
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
            "DELETE_SCENARIO"  : self.delete_func,
            "INGEST_SCENARIO"  : self.ingest_func,
            "INGEST_LOCAL_PROD": self.local_product_func,
            "ADD_PRODUCT"      : add_product_wfunc       # from add_product
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

    def mk_s2pre_scriptandargs(self, s2pre, targetdir, s2meta):
        if s2pre == 'NO':
            return []

        if s2pre == 'AT':
            return self.mk_s2atm_scriptandargs(targetdir, s2meta)

        else:
            self._logger.error("S2 preprocessing type "+`s2pre`+
                               " is not implememented")
            return {}


    def mk_s2atm_scriptandargs(self, targetdir, s2meta):
        metalist = []
        print "s2meta: " + `s2meta`
        if s2meta:
            if not s2meta.endswith(IE_DIMAPMETA_SUFFIX):
                self._logger.warning(
                    "metadata file for s2 preprocessing does not end in "
                    + IE_DIMAPMETA_SUFFIX)
            metalist = [s2meta]
        else:
            metalist = get_glob_list(targetdir, IE_DIMAPMETA_SUFFIX)

        retlist = []
        
        s2atm_script = os.path.join(IE_SCRIPTS_DIR, IE_S2ATM_PREPROCESS_SCRIPT)
        targetdir_str = '-targetdir=' + targetdir
        beam_home_str = '-beam_home=' + IE_BEAM_HOME
        for m in metalist:
            base = os.path.splitext(m)[0]
            outfile_str = '-outfile=' + base + IE_S2ATM_OUT_SUFFIX
            meta_str = '-meta=' + m
            retlist.append((s2atm_script,
                            targetdir_str,
                            meta_str,
                            outfile_str,
                            beam_home_str))
        return retlist

    def mk_catreg_arg(self):
        return "-catreg=" + \
                os.path.join(IE_SCRIPTS_DIR, IE_DEFAULT_CATREG_SCRIPT)

    
    def mk_scripts_args(self,
                        scripts,
                        mf_name,
                        cat_reg):
        scripts_args = []
        if cat_reg:
            cat_reg_str = self.mk_catreg_arg()
        for s in scripts:
            if cat_reg:
                scripts_args.append([s, mf_name, cat_reg_str])
            else:
                scripts_args.append([s, mf_name])
        return scripts_args

    def run_scripts(self, sc_id, ncn_id, scripts_args):
        if not scripts_args: return 0

        n_errors = 0
        for script_arg in scripts_args:
            if check_status_stopping(sc_id):
                raise StopRequest("Stop Request")

            self._logger.info("Running script: %s" % script_arg[0])
            r = subprocess.call(script_arg)
            if 0 != r:
                n_errors += 1
                self._logger.error(`ncn_id`+": script returned status:"+`r`)
        return n_errors

    def post_download_actions(self,
                              scid,
                              ncn_id,
                              dl_dir,
                              scripts,
                              cat_reg,
                              s2pre,
                              tar_result):
        # For each product that was downloaded into its seperate
        # directory, generate a product manifest for the ODA server,
        # and also split each downloaded product into its parts.
        # Then run the post- ingestion scripts.
        # TODO: the splitting could be done by the EO-WCS DM plugin
        #       instead of doing it here
        dir_list = os.listdir(dl_dir)
        n_dirs = len(dir_list)
        n_errors = 0
        i = 1
        for d in dir_list:
            percent  = 100 * (float(i) / float(n_dirs))
            # keep percent > 0 to ensure webpage updates
            if percent < 1.0: percent = 1
            self._wfm.set_scenario_status(self._id, scid, 0, "RUNNING SCRIPTS", percent)

            try:
                mf_name, metafiles = split_and_create_mf(
                    dl_dir, d, ncn_id, self._logger)
            except Exception as e:
                self._logger.info("Exception" + `e`)
                mf_name = None
            if not mf_name:
                self._logger.info("Error processing download directry " + `d`)
                n_errors += 1
                continue

            # archive products that were downloaded
            for m in metafiles:
                archive_metadata(scid, m)

            scripts_args = self.mk_scripts_args(
                scripts, mf_name, cat_reg)
            n_errors += self.run_scripts(scid, ncn_id, scripts_args)

            i += 1

        # run the tar script if requested
        if tar_result:
            if check_status_stopping(scid):
                raise StopRequest("Stop Request")

            tar_script = os.path.join(IE_SCRIPTS_DIR, IE_TAR_RESULT_SCRIPT)
            script_arg = [tar_script, dl_dir]
            if cat_reg:
                script_arg.append(self.mk_catreg_arg())
            self._logger.info(`ncn_id`+": running " + `script_arg`)
            r = subprocess.call(script_arg)
            if 0 != r:
                n_errors += 1
                self._logger.error(`ncn_id`+": tar script returned status:"+`r`)
            else:
                self._logger.info(`ncn_id`+": tar file is ready: " +
                                  dl_dir + IE_TAR_FILE_SUFFIX);

        return n_errors


    def do_task(self,current_task):

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

    def delete_func(self, parameters):
        scid = parameters["scenario_id"]
        self._wfm._lock_db.acquire()
        
        try:
            scenario = models.Scenario.objects.get(id=int(scid))
            ncn_id = scenario.ncn_id
            
            scenario_status = models.ScenarioStatus.objects.get(scenario_id=int(scid))
            status = scenario_status.status
            avail  = scenario_status.is_available
            dar    = scenario_status.active_dar
            if dar != '':
                self._logger.error(
                    `ncn_id`+ 
                    ": Cannot delete, scenario has an active DAR," +
                    " must be stopped first.")
                if IE_DEBUG > 0:
                    self._logger.debug("  dar="+`dar`)
                    scenario_status.status = "NOT DELETED - ERROR."
                    scenario_status.is_available = 1
                    scenario_status.done = 0
                    scenario_status.save()
                return

            scenario_status.is_available = 0
            scenario_status.status = "DELETE: De-reg products."
            scenario_status.done = 1
            scenario_status.save()

            is_cat_reg = scenario.cat_registration

            #this may run for a long time, and the db is locked, but
            #there is nothing to be done here..
            n_errors = 0
            try:
                for script in parameters["scripts"]: # scripts absolute path
                    self._logger.info(`ncn_id`+" del running script "+`script`)
                    if is_cat_reg:
                        args = [script, ncn_id,
                                "-catreg=%s"%os.path.join(IE_SCRIPTS_DIR, IE_DEFAULT_CATDEREG_SCRIPT)]
                    else:
                        args = [script, ncn_id]
                    r = subprocess.call(args)
                    if 0 != r:
                        n_errors += 1
                        self._logger.error(
                            `ncn_id`+": delete script returned status:"+`r`)
            except Exception as e:
                n_errors += 1
                self._logger.error(`ncn_id`+": Exception while deleting: "+`e`)

            if n_errors > 0:
                scenario_status.status = "NOT DELETED - ERROR."
                scenario_status.is_available = 1
                scenario_status.done = 0
                scenario_status.save()
                return
                
            scenario_status.status = "DELETING"
            scenario_status.save()

            # delete scenario and all associated data from the db 
            scripts = scenario.script_set.all()
            delete_scripts(scripts)

            scenario.delete()
            scenario_status.delete()

        except Exception as e:
            self._logger.error(`e`)
        finally:
            self._wfm._lock_db.release()

    def local_product_func(self,parameters):
        if IE_DEBUG > 0:
            self._logger.info(
                "wfm: executing INGEST LOCAL PRODUCT, id=" +\
                    `parameters["scenario_id"]`)

        percent = 1
        ncn_id = None
        n_errors = 0
        try:
            sc_id = parameters["scenario_id"]
            self._wfm.set_scenario_status(
                self._id, sc_id, 0, "LOCAL ING.: UNPACK", percent)
            self._wfm.set_ingestion_pid(sc_id, os.getpid())
            ncn_id = parameters["ncn_id"].encode('ascii','ignore')

            data = parameters["data"]
            orig_data = None
            data = ie_unpack_maybe(parameters["dir_path"], data)
            if not data:
                raise IngestionError(
                    "Error unpacking or accessing " +
                    os.path.join(parameters["dir_path"]), data)

            if 'NO' != parameters["s2_preprocess"]:
                s2script_args = self.mk_s2pre_scriptandargs(
                    parameters["s2_preprocess"],
                    parameters["dir_path"],
                    parameters["metadata"])

                if s2script_args:

                    self._wfm.set_scenario_status(
                        self._id, sc_id, 0, "LOCAL ING.: S2-PRE", percent)
            
                    s2pre_errors = self.run_scripts(sc_id, ncn_id, s2script_args)
                    if s2pre_errors > 0:
                        n_errors += s2pre_errors
                    else:
                        orig_data = data
                        data = extract_outfile(s2script_args[0][3])

            mf_name = create_manifest(
                self._logger,
                ncn_id,
                parameters["dir_path"],
                metadata=parameters["metadata"],
                data=data,
                orig_data=orig_data
                )

            self._wfm.set_scenario_status(
                self._id, sc_id, 0, "RUNNING SCRIPTS", percent)

            scripts_args = self.mk_scripts_args(
                parameters["scripts"],
                mf_name,
                parameters["cat_registration"])

            n_errors += self.run_scripts(sc_id, ncn_id, scripts_args)

            if n_errors > 0:
                raise IngestionError("Number of errors " +`n_errors`)
            self._wfm.set_scenario_status(self._id, sc_id, 1, "IDLE", 0)
            self._logger.info("Local ingestion completed, dir: " +
                              parameters["dir_path"])

        except StopRequest as e:
            self._logger.info(`ncn_id`+
                              ": Stop request from user: Local Ingestion Stopped")
            self._wfm.set_scenario_status(self._id, sc_id, 1, "IDLE", 0)

        except Exception as e:
            self._logger.error(`ncn_id`+" Error while ingesting local product: " + `e`)
            self._wfm.set_scenario_status(self._id, sc_id, 1, "INGEST ERROR", 0)
            if IE_DEBUG > 0:
                traceback.print_exc(12,sys.stdout)

        finally:
            self._wfm.set_ingestion_pid(sc_id, 0)


    def ingest_func(self,parameters):
        if IE_DEBUG > 0:
            self._logger.info(
                "wfm: executing INGEST_SCENARIO, id=" +\
                    `parameters["scenario_id"]`)

        percent = 1
        sc_id = parameters["scenario_id"]
        ncn_id = None
        final_status = "OK"
        self._wfm.set_scenario_status(
            self._id, sc_id, 0, "GENERATING URLS", percent)
        try:
            scenario = models.Scenario.objects.get(id=sc_id)
            ncn_id   = scenario.ncn_id.encode('ascii','ignore')
            cat_reg  = scenario.cat_registration

            # ingestion_logic blocks until DM is finished downloading
            self._wfm.set_ingestion_pid(sc_id, os.getpid())
            dl_errors, dl_dir, dar_url, dar_id, status = \
                ingestion_logic(sc_id, models.scenario_dict(scenario))

            if check_status_stopping(sc_id):
                raise StopRequest("Stop Request")

            n_errors = 0
            if status == "NO_ACTION":
                final_status = "NOTHING INGESTED"
            else:
                if None == dar_id:
                    raise IngestionError("No DAR generated")

                s2pre = scenario.s2_preprocess
                if s2pre != 'NO':
                    # s2pre is functional only for local ingestion
                    self._logger.error(
                        "S2 Preprocessor is not implemented for data from product facility."+
                        " Hint: use local ingestion instead")
                    s2pre = 'NO'

                n_errors = self.post_download_actions(
                    sc_id,
                    ncn_id,
                    dl_dir,
                    parameters["scripts"],
                    cat_reg,
                    s2pre,
                    scenario.tar_result)

            n_errors += dl_errors
            if n_errors>0:
                raise IngestionError(`ncn_id`+": ingestion encountered "+ `n_errors` +" errors")

            # Finished
            if "OK" == final_status:
                d_str = time.strftime('%Y-%m-%d %H:%M', time.gmtime())
                final_status += ' ' + d_str
            self._wfm.set_scenario_status(self._id, sc_id, 1, final_status, 0)
            self._logger.info(`ncn_id`+": ingestion completed.")

        except StopRequest as e:
            self._logger.info(`ncn_id`+": Stop request from user: Ingestion Stopped")
            self._wfm.set_scenario_status(self._id, sc_id, 1, "STOPPED, IDLE", 0)

        except Exception as e:
            self._logger.error(`ncn_id`+" Error while ingesting: " + `e`)
            self._wfm.set_scenario_status(self._id, sc_id, 1, "INGEST ERROR", 0)
            if IE_DEBUG > 0:
                traceback.print_exc(12,sys.stdout)

        finally:
            self._wfm.set_ingestion_pid(sc_id, 0)


#**************************************************
#      Auto-Ingest-Scenario-Worker                *
#**************************************************
class AISWorker(threading.Thread):
    #
    # manages auto-ingestion of scenario
    #

    def __init__(self,work_flow_manager):
        threading.Thread.__init__(self)
        global worker_id
        self._id = worker_id
        worker_id = worker_id + 1
        self._wfm = work_flow_manager
        self._logger = logging.getLogger('dream.file_logger')

    def run(self):
        #
        # manages auto-ingestion of scenar
        #

        if IE_DEBUG > 1: self._logger.debug(
            "AISWorker-%d of Work-Flow Manager started." % self._id)
        while True:
            # read all scenarios
            self._wfm.lock_db()
            scenarios = models.Scenario.objects.all()

            for scenario in scenarios:

                if scenario.repeat_interval==0:
                    continue

                # use the following for tz-aware datetimes
                #t_now = datetime.datetime.utcnow().replace(tzinfo=utc)
                t_now = datetime.datetime.utcnow()

                if scenario.starting_date > t_now:
                    continue

                scid = scenario.id
                scenario_status = models.ScenarioStatus.objects.get(scenario_id=scid)
                if scenario_status.is_available != 1:
                    self._logger.warning("Attempt to run auto scenario "
                                         + `scenario.ncn_id`
                                         + " but status.is_available = "
                                         + `scenario_status.is_available`
                                         + ", will try later.")
                    continue

                if IE_DEBUG > 1:
                    self._logger.debug (
                        "Auto-ingest: Scenario: " + `scenario.id` +
                        ", starting_date: " + scenario.starting_date.isoformat() +
                        ", repeat_interval: " + `scenario.repeat_interval` + " minutes"
                        )

                t_now = datetime.datetime.utcnow()
                
                # set the next starting date
                next_start = t_now + datetime.timedelta(0, 60*scenario.repeat_interval)
                scenario.starting_date = next_start
                scenario.save()

                scenario_status.is_available = 0
                scenario_status.status = "QUEUED"
                scenario_status.save()

                if IE_DEBUG > 0:
                    self._logger.debug (
                        "Auto-ingest: Submitting scenario '%s' to queue" % scenario.ncn_id
                        )
                submit_scenario(scenario, scid, False)

                    # t_prev = t_now - t_delta
                    # if scenario.starting_date <= t_prev:
                    #     scenario.starting_date = t_prev
                    # while scenario.starting_date <= t_now:
                    #     scenario.starting_date += t_delta
                    #     if IE_DEBUG > 2: self._logger.debug (
                    #         "Scenario %d - new time: %s" % \
                    #             (scenario.id,scenario.starting_date))
                    # # put task to queue to process
                    # ingest_scripts = []
                    # scripts = scenario.script_set.all()
                    # for s in scripts:
                    #     ingest_scripts.append("%s" % s.script_path)
                    # current_task =  WorkerTask(
                    #     {"scenario_id":scenario.id,
                    #      "task_type":"INGEST_SCENARIO",
                    #      "scripts":ingest_scripts})
                    # scenario.save() # save updated starting_date
                    # self._wfm.put_task_to_queue(current_task)

            self._wfm.release_db()
            time.sleep(60) # repeat checking every 1 minute


#**************************************************
#               Work-Flow-Manager                 *
#**************************************************
@Singleton
class WorkFlowManager:
    def __init__(self):
        self._queue = Queue.LifoQueue() # first items added are first retrieved

        self._workers = []
        n = 0
        while n < IE_N_WORKFLOW_WORKERS:
            self._workers.append(Worker(self))
            n += 1

        self._AIS_worker = AISWorker(self)

        self._lock_db = threading.Lock()
        self._logger = logging.getLogger('dream.file_logger')

    def lock_db(self):
        self._lock_db.acquire()

    def release_db(self):
        self._lock_db.release()

    def put_task_to_queue(self,current_task):
        if isinstance(current_task,WorkerTask):
            self._queue.put(current_task)
        else:
             self._logger.error ("Current_task is not a task.")

    def set_ingestion_pid(self, scid, pid):
        self._lock_db.acquire()
        try:
            ss = models.ScenarioStatus.objects.get(scenario_id=scid)
            ss.ingestion_pid = pid
            ss.save()
        except Exception as e:
            self._logger.error(`e`)
        finally:
            self._lock_db.release()
        
    def set_active_dar(self, scid, dar_id):
        # Also used for concurrency control.  There should be only one
        # active dar per scenario.  If it is not
        # empty and we are trying to set another one, we return False.
        # If it was empty there is no active dar underway, so
        # if we are trying to clear it again we also return false.
        #
        self._lock_db.acquire()
        try:
            ss = models.ScenarioStatus.objects.get(scenario_id=scid)
            old_dar = ss.active_dar
            if dar_id and old_dar:
                raise IngestionError(
                    "A DAR is already ative for scenario "+`scid`)
            if not dar_id and not old_dar:
                raise StopRequest('')
            ss.active_dar = dar_id
            ss.save()

        except StopRequest as e:
            return False

        except IngestionError as e:
            self._logger.error(`e`)
            return False

        except Exception as e:
            self._logger.error(`e`)

        finally:
            self._lock_db.release()
        return True

    def set_stop_request(self, scenario_id):
        self._lock_db.acquire()
        try:
            scenario_status = models.ScenarioStatus.objects.get(
                scenario_id=scenario_id)
            # set stop request only if ingestion is active,
            # otherwise set to IDLE
            active_dar = scenario_status.active_dar
            pid = scenario_status.ingestion_pid
            if pid != os.getpid():
                pid = 0
            if active_dar or pid!=0:
                scenario_status.status = STOP_REQUEST
                scenario_status.is_available = 1
                scenario_status.active_dar = ''
            else:
                scenario_status.status = 'IDLE'
                scenario_status.is_available = 1
                scenario_status.done = 0
            scenario_status.save()
        except Exception as e:
            self._logger.error(`e`)
        finally:
            self._lock_db.release()
        if active_dar:
            stop_active_dar_dl(active_dar)

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

    def lock_scenario(self, scenario_id):
        self._lock_db.acquire()
        try:
            # set scenario status
            scenario_status = models.ScenarioStatus.objects.get(
                scenario_id=scenario_id)
            if scenario_status.is_available != 1:
                return False
            scenario_status.is_available = 0
            scenario_status.save()
        except Exception as e:
            self._logger.error(`e`)
        finally:
            self._lock_db.release()
        return True

    def start(self):
        self._AIS_worker.start()
        for w in self._workers:
            w.setDaemon(True)
            w.start()
