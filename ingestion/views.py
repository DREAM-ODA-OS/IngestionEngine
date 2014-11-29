############################################################
#  Project: DREAM
#  Module:  Task 5 ODA Ingestion Engine 
#  Authors:  Vojtech Stefka  (CVC), Milan Novacek (CVC)
#
#    (c) 2013 Siemens Convergence Creators s.r.o. (CVC), Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
#  Ingestion Engine Django Views setup.
#
############################################################

from django.template.loader import get_template
from django.shortcuts import render_to_response,render
from django.template import RequestContext
from django.http import HttpResponseRedirect,HttpResponse
from django.contrib.auth import logout
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.models import User
from django.views.defaults import server_error
from django.utils.timezone import utc
from django.contrib.auth import authenticate, login
from django.http import Http404
from django.forms.util import ErrorList
from urllib2 import URLError
from ingestion_logic import create_dl_dir

import os
import logging
import json
import re

import models
import forms
import work_flow_manager

from settings import \
    IE_DEBUG, \
    set_autoLogin, \
    IE_HOME_PAGE, \
    IE_AUTO_LOGIN, \
    MEDIA_ROOT, \
    IE_SCRIPTS_DIR, \
    IE_DEFAULT_INGEST_SCRIPT, \
    IE_DEFAULT_ADDPROD_SCRIPT, \
    IE_DEFAULT_DEL_SCRIPT, \
    JQUERYUI_OFFLINEURL, \
    SC_NCN_ID_BASE, \
    AUTHENTICATION_BACKENDS

from utils import \
    read_from_url, \
    ManageScenarioError

from dm_control import \
    DownloadManagerController, \
    DM_DAR_STATUS_COMMAND

from uqmd import updateMetaData

from add_product import add_product_submit


IE_DEFAULT_USER = r'drtest'
IE_DEFAULT_PASS = r'1234'

IE_RE_NCN_FORBIDDEN = re.compile(r"[/:|%@]")

dmcontroller = DownloadManagerController.Instance()
logger = logging.getLogger('dream.file_logger')


def get_scenario_script_paths(scenario):
    # get list of scripts
    scripts = scenario.script_set.all()
    ingest_scripts = []
    if scenario.oda_server_ingest != 0:
        ingest_scripts.append(os.path.join(IE_SCRIPTS_DIR, IE_DEFAULT_INGEST_SCRIPT) )
    for s in scripts:
        ingest_scripts.append("%s" % s.script_path)

    return ingest_scripts


def submit_scenario(scenario, scenario_id, set_status=True):
    wfm = work_flow_manager.WorkFlowManager.Instance()
    scripts = get_scenario_script_paths(scenario)
    if len(scripts) == 0:
        msg = "Scenario '%s' name=%s does not have any scripts." \
            % (scenario.ncn_id, scenario.scenario_name)
        logger.warning(msg)

    # send request/task to work-flow-manager to run ingestion
    current_task = work_flow_manager.WorkerTask(
        {"scenario_id":scenario_id,
         "task_type":"INGEST_SCENARIO",
         "scripts":scripts},
        set_status)

    wfm.put_task_to_queue(current_task)
    logger.info(
        'Operation: ingest scenario: id=%d name=%s' \
            % (scenario.id,scenario.scenario_name))

def get_scenario_id(ncn_id):
    return models.Scenario.objects.get(ncn_id=ncn_id).id

def get_scenario_ncn_id(scenario_id):
    return models.Scenario.objects.get(id=int(scenario_id)).ncn_id

def get_or_create_scenario_status(s):
    sstat = None
    try:
        sstat = s.scenariostatus
    except models.ScenarioStatus.DoesNotExist:
        sstat = models.ScenarioStatus(
            scenario=s,
            is_available=1,
            status='IDLE',
            ingestion_pid=os.getpid(),
            done=0.0)
        sstat.save()
    return sstat

def stop_ingestion_core(scenario_id):
    wfm = work_flow_manager.WorkFlowManager.Instance()
    wfm.set_stop_request(scenario_id)

def reset_scenario_core(ncn_id):
    ret = {}

    scenario = models.Scenario.objects.get(ncn_id=ncn_id)
    scenario_id = scenario.id

    wfm = work_flow_manager.WorkFlowManager.Instance()

    if not wfm.lock_scenario(scenario_id):
        msg = "Scenario '%s' name=%s is busy." % (ncn_id, scenario.scenario_name)
        logger.warning("Delete Scenario refused: " + msg)
        ret = {'status':1, 'message':"Error: "+msg}
    else:
        default_delete_script = os.path.join(IE_SCRIPTS_DIR, IE_DEFAULT_DEL_SCRIPT)
        del_scripts = [default_delete_script]
        scripts = scenario.script_set.all()
        for s in scripts:
            del_scripts.append("%s/%s" % (MEDIA_ROOT,s.script_file))

        current_task = work_flow_manager.WorkerTask(
            {"scenario_id":scenario_id,
             "task_type":"RESET_SCENARIO",
             "scripts":del_scripts})
        wfm.put_task_to_queue(current_task)
        
        ret = {'status':0,}

    return ret

def delete_scenario_core(scenario_id=None, ncn_id=None):
    ret = {}

    if ncn_id:
        ncn_id = ncn_id.encode('ascii','ignore')
        scenario = models.Scenario.objects.get(ncn_id=ncn_id)
        scenario_id = scenario.id
    else:
        scenario = models.Scenario.objects.get(id=int(scenario_id))
        ncn_id   = scenario.ncn_id

    # send request/task to work-flow-manager to run delete script
    wfm = work_flow_manager.WorkFlowManager.Instance()

    if not wfm.lock_scenario(scenario_id):
        msg = "Scenario '%s' name=%s is busy." % (ncn_id, scenario.scenario_name)
        logger.warning("Delete Scenario refused: " + msg)
        ret = {'status':1, 'message':"Error: "+msg}
    else:

        logger.info ("Submitting to Work-Queue: delete scenario ncn_id="+`ncn_id`)
        default_delete_script = os.path.join(
            IE_SCRIPTS_DIR, IE_DEFAULT_DEL_SCRIPT)
        #del_scripts = models.UserScript.objects.filter(
        #    script_name__startswith="deleteScenario-",user_id__exact=user.id)
        del_scripts = [default_delete_script]
        scripts = scenario.script_set.all()
        for s in scripts:
            del_scripts.append("%s/%s" % (MEDIA_ROOT,s.script_file))
        if len(del_scripts)==0:
            logger.warning('Scenario: id=%d name=%s does not have a delete script.' \
                               % (scenario.id,scenario.scenario_name))

        current_task = work_flow_manager.WorkerTask(
            {"scenario_id":scenario_id,
             "task_type":"DELETE_SCENARIO",
             "scripts":del_scripts})
        wfm.put_task_to_queue(current_task)
        
        ret = {'status':0,}

    return ret


def ingest_scenario_core(scenario_id=None, ncn_id=None):
    # ingest scenario - run all ing. scripts related to the scenario
    logger.info("Submitting ingest request to Ingestion Engine Worker queue")
    ret = {}

    if ncn_id:
        scenario = models.Scenario.objects.get(ncn_id=ncn_id)
        scenario_id = scenario.id
    else:
        scenario = models.Scenario.objects.get(id=int(scenario_id))
        ncn_id   = scenario.ncn_id

    if scenario.repeat_interval > 0:
        msg = "Cannot manually run an auto-ingest scenario (repeat interval >0)."
        logger.warning("Attempt to manually run Auto-Scenario: " + msg)
        ret = {'status':1, 'message':"Error: "+msg}
        return ret
        

    wfm = work_flow_manager.WorkFlowManager.Instance()
    if not wfm.lock_scenario(scenario_id):
        msg = "Scenario '%s' name=%s is busy." % (scenario.ncn_id, scenario.scenario_name)
        logger.warning("Ingest Scenario refused: " + msg)
        ret = {'status':1, 'message':"Error: "+msg}
    else:
        submit_scenario(scenario, scenario_id)

        ret = {'status':0,
               "message":"Ingestion Submitted to processing queue."}

    return ret


@csrf_exempt
def do_post_operation(op, request):
    # must be a POST request
    response_data = {}

    if request.method == 'POST':
        response_data = op(request.body, request.META)
        return HttpResponse(
            json.dumps(response_data),
            content_type="application/json")

    else:
        logger.error("Unxexpected GET request on url "+`request.path_info`)
        raise Http404

def auto_login(request):
    if not IE_AUTO_LOGIN:
        return

    new_user = authenticate(
        username=IE_DEFAULT_USER,
        password=IE_DEFAULT_PASS)
    if None == new_user:
        logger.warn('Cannot log in user '+IE_DEFAULT_USER)
    else:
        new_user.backend=AUTHENTICATION_BACKENDS[0]
        login(request, new_user)
        logger.info('Auto-logged-in ' + IE_DEFAULT_USER+".")
    return new_user
    
def oda_init(request):
    port = request.META['SERVER_PORT']
    dmcontroller.set_ie_port(port)
    user = request.user
    if user.username != IE_DEFAULT_USER:
        user = auto_login(request)

def main_page(request):
    dmcontroller.set_ie_port(request.META['SERVER_PORT'])
    user = request.user

    if user.username != IE_DEFAULT_USER:
        user = auto_login(request)

    elif IE_AUTO_LOGIN and \
            (not user.is_authenticated()) and \
            request.path == '/'+IE_HOME_PAGE:
        user = auto_login(request)

    return render_to_response(
        'base.html',
        {'user': user,
         'home_page':IE_HOME_PAGE,
         'jqueryui_offlineurl': JQUERYUI_OFFLINEURL
         })

def logout_page(request):
    settings.set_autoLogin(False)
    logout(request)
    return HttpResponseRedirect('/')


def overviewScenario(request):
    # retrieve scenarios for current user
    user = request.user
    if not request.user.username == IE_DEFAULT_USER:
        user = auto_login(request)

    scenarios = user.scenario_set.all()
    scenario_status = []
    for s in scenarios:
        sstat = get_or_create_scenario_status(s)
        scenario_status.append(sstat)

    variables = RequestContext(
        request,
        {'scenarios':scenarios,
         'scenario_status':scenario_status,
         'home_page':IE_HOME_PAGE,
         'jqueryui_offlineurl': JQUERYUI_OFFLINEURL
         })

    return render_to_response('overviewScenario.html',variables)


def delete_scripts(scripts):
    for s in scripts:
        try:
            os.remove(s.script_path) # delete script related to current scenario
            script = models.Script.objects.get(script_path=s.script_path)
            script.delete()
        except Exception as e:
            logger.error("Deletion error: " + `e`, extra={'user':"drtest"})

def delete_all_eoids(scenario):
    existing = scenario.eoid_set.all()
    for e in existing:
        e.delete()

def delete_eoids(scenario, del_list):
    for e in del_list:
        try:
            eoid = models.Eoid.objects.get(eoid_val=e)
            eoid.delete()
        except Exception as e:
            logger.error("Internal error housekeeping for EOIDS," + `e`)
        
def  refresh_dssids_chache(scenario, new_dssids, selected_dssids):
    # Caches in the db dssids that were presumably obtained from the pf
    #  if any dss ids already exist in the db, then the old ones are
    #  deleted and replaced by new dssids.  The selected flag is
    #  set to True for any dssid that is in the selected_dssids list.

    scenario.eoid_set.all().delete()

    for e in new_dssids:

        if e in selected_dssids: this_sel = True
        else:                    this_sel = False

        eoid = models.Eoid(scenario=scenario,
                    eoid_val=e.encode('ascii','ignore'),
                    selected=this_sel)
        eoid.save()

def reset_dssid_selection(scenario, selected_dssids):
    old_selection = scenario.eoid_set.filter(selected=True)
    for eoid in old_selection:
        eoid.selected=False
        eoid.save()

    for e in selected_dssids:
        eoid = models.Eoid.objects.get(eoid_val=e,scenario=scenario)
        eoid.selected=True
        eoid.save()

def empty_dssids(ids):
    if len(ids) == 0: return True
    if ids == '':     return True
    if len(ids) == 1 and (ids[0] == '' or len(ids[0]) == 0): return True
    return False

def handle_eoids(request,scenario):

    if not 'eoid_selected' in request.POST or \
            not 'all_dssids'  in request.POST:
        return

    all_dssids = request.POST['all_dssids'].encode('ascii','ignore').split(',')
    
    selected_dssids = request.POST.getlist(u'eoid_selected')

    wfm = work_flow_manager.WorkFlowManager.Instance()
    wfm._lock_db.acquire()

    try:

        if not empty_dssids(all_dssids):
            refresh_dssids_chache(scenario, all_dssids, selected_dssids)
        else:
            reset_dssid_selection(scenario, selected_dssids)

    except Exception as e:
        logger.error(`e`)

    finally:
        wfm._lock_db.release()


def handle_extras(request,scenario):
    xpaths = request.POST.getlist('extra_xpath')

    if len(xpaths) == 0:
        return

    texts  = request.POST.getlist('extra_text')
    
    extras = scenario.extraconditions_set.all()
    for e in extras:
        e.delete()
    extras = None

    i = 0
    for x in xpaths:
        if not x: continue
        models.ExtraConditions(
            scenario=scenario,
            xpath=x.encode('ascii','ignore'),
            text=texts[i].encode('ascii','ignore'),
            ).save()
        i += 1

def handle_uploaded_scripts(request,scenario):
    if len(request.FILES)==0:
        return
    scripts = scenario.script_set.all()
    delete_scripts(scripts)
    names = request.POST.getlist('script_name')
    i = 0
    for filename in request.FILES: # files is dictionary
        f = request.FILES[filename] 
        file_path = "%s/scripts/%s_%s_%d" % \
            (MEDIA_ROOT, str(request.user.id), str(scenario.id), i)
        try:
            destination = open(file_path,'w')
            if f.multiple_chunks:
                for chunk in f.chunks():
                    destination.write(chunk)
            else:
                destination.write(f.read())
        except Exception as e:
            logger.error("Upload error: " + `e`, extra={'user':request.user})
        finally:
            destination.close()
        script = models.Script()
        script.script_name = names[i]
        script.script_path = file_path
        script.scenario = scenario
        script.save()
        i = i + 1

def ncn_is_valid(form, scid):
    # make sure the ncn_id is unique and does not contain bad chars
    form_ncn_id = form.data['ncn_id']
    is_ncn_ok = True
    errs = []

    if None != IE_RE_NCN_FORBIDDEN.search(form_ncn_id):
        is_ncn_ok = False
        errs.append("Forbidden characters found in scenario Id.")

    try:
        ex_sc = models.Scenario.objects.get(ncn_id=form_ncn_id)
        if (scid and ex_sc.id != int(scid)) or not scid:
            is_ncn_ok = False
            errs.append("scenario Id is not unique, please choose a different one.")
    except models.Scenario.DoesNotExist:
        # all is well
        pass
    
    if not is_ncn_ok:
            if None == form._errors:
                form._errors = {}
            form._errors['ncn_id'] = ErrorList()
            for e in errs:
                form._errors['ncn_id'].append(e)
        
    return is_ncn_ok

def init_status(scenario):
    scenario_status = models.ScenarioStatus()
    scenario_status.scenario = scenario
    scenario_status.is_available = 1
    scenario_status.status = "IDLE"
    scenario_status.done = 0
    scenario_status.ingestion_pid = 0
    scenario_status.active_dar = ''
    scenario_status.save()


def addLocalProductOld(request, sc_id):
    # add local product to the related scenario
    port = request.META['SERVER_PORT']

    msg = "The new product has been successfully added"
    if request.method == 'POST':

        wfm = work_flow_manager.WorkFlowManager.Instance()
        scenario = models.Scenario.objects.get(id=int(sc_id))

        if not wfm.lock_scenario(sc_id):
            logger.warning("Ingest Scenario refused: "
                + "Scenario '%s' name=%s is busy." % (scenario.ncn_id, scenario.scenario_name))
            return HttpResponseRedirect('http://127.0.0.1:'+port+'/scenario/overview/')

        form = forms.AddLocalProductForm(request.POST, request.FILES)
        if form.is_valid() and form.is_multipart():
            try:
                sc_ncn_id = scenario.ncn_id
                full_directory_name, directory_name = create_dl_dir(sc_ncn_id+"_")
                full_directory_name = full_directory_name.encode('ascii','ignore')
                logger.info("Product directory name:" + full_directory_name)
            except:
                wfm.set_scenario_status(0, sc_id, 1, 'IDLE', 0)
                logger.error("Failed to generate a new product directory name")
                return HttpResponseRedirect('http://127.0.0.1:'+port+'/scenario/overview/')
            try:
                saveFile(request.FILES['metadataFile'], full_directory_name)
            except IOError, e:
                wfm.set_scenario_status(0, sc_id, 1, 'IDLE', 0)
                logger.error("Failed to save product's metadata file")
                return HttpResponseRedirect('http://127.0.0.1:'+port+'/scenario/overview/')
            try:
                saveFile(request.FILES['rasterFile'], full_directory_name)
            except IOError, e:
                wfm.set_scenario_status(0, sc_id, 1, 'IDLE', 0)
                logger.error("Failed to save product's raster file")
                return HttpResponseRedirect('http://127.0.0.1:'+port+'/scenario/overview/')

            scripts = get_scenario_script_paths(scenario)
            if len(scripts) > 0:
                # send request/task to work-flow-manager to run script
                current_task = work_flow_manager.WorkerTask(
                    {"scenario_id": sc_id,
                    "ncn_id"   : sc_ncn_id,
                    "task_type": "INGEST_LOCAL_PROD",
                    "scripts"  : scripts,
                    "cat_registration"  : scenario.cat_registration,
                     "s2_preprocess"    : scenario.s2_preprocess,
                    "dir_path" : full_directory_name,
                    "metadata" : request.FILES['metadataFile']._get_name(),
                    "data"     : request.FILES['rasterFile']._get_name(),
                    })

                wfm.put_task_to_queue(current_task)
                logger.info('Operation: ingest scenario: id=%d name=%s' \
                        % (scenario.id,scenario.scenario_name))
            else:
                logger.error("NOT INGESTING: Scenario '%s' name=%s does not have scripts to ingest." \
                        % (scenario.ncn_id,scenario.scenario_name))
                wfm.set_scenario_status(0, sc_id, 1, 'IDLE', 0)

            return HttpResponseRedirect('http://127.0.0.1:'+port+'/scenario/overview/')
        else:
            wfm.set_scenario_status(0, sc_id, 1, 'IDLE', 0)
            logger.warning('The add product form has not been fully/correctly filled')
            return HttpResponseRedirect('http://127.0.0.1:'+port+'/scenario/overview/')
    else:
        form = forms.AddLocalProductForm()
        variables = RequestContext(request, {
                'form'     :form,
                'home_page':IE_HOME_PAGE})
        return render_to_response('addLocalProduct.html', variables)

def saveFile(upload_file, path):
    # saves a file
    filename = upload_file._get_name()
    path_file_name = os.path.join(path, filename)
    logger.info('Saving file: ' + path_file_name)
    fd = open(path_file_name, 'wb')
    for chunk in upload_file.chunks():
        fd.write(chunk)
    fd.close()

def odaAddLocalProductOld(request, ncn_id):
    oda_init(request)
    # add local product to the related scenario
    logger.info("addLocalProduct to ncn_id "+ncn_id)

    msg = "The new product has been successfully added"
    if request.method == 'POST':

        wfm = work_flow_manager.WorkFlowManager.Instance()
        scenario = models.Scenario.objects.get(ncn_id)
        sc_id  = scenario.id

        #if not wfm.lock_scenario(sc_id):
        #    return getAddProductResult(request, sc_id, msg)
        if not wfm.lock_scenario(sc_id):
            logger.warning("Ingest Scenario refused: "
                + "Scenario '%s' name=%s is busy." % (scenario.ncn_id, scenario.scenario_name))
            return HttpResponseRedirect('http://127.0.0.1:'+port+'/scenario/overview/')

        form = forms.AddLocalProductForm(request.POST, request.FILES)
        if form.is_valid() and form.is_multipart():
            try:
                sc_ncn_id = scenario.ncn_id
                full_directory_name, directory_name = create_dl_dir(sc_ncn_id+"_")
                full_directory_name = full_directory_name.encode('ascii','ignore')
                logger.info("Product directory name:" + full_directory_name)
            except:
                wfm.set_scenario_status(0, sc_id, 1, 'IDLE', 0)
                logger.error("Failed to generate a new product directory name")
                return HttpResponseRedirect('http://127.0.0.1:'+port+'/scenario/overview/')
            try:
                saveFile(request.FILES['metadataFile'], full_directory_name)
            except IOError, e:
                wfm.set_scenario_status(0, sc_id, 1, 'IDLE', 0)
                logger.error("Failed to save product's metadata file")
                return HttpResponseRedirect('http://127.0.0.1:'+port+'/scenario/overview/')
            try:
                saveFile(request.FILES['rasterFile'], full_directory_name)
            except IOError, e:
                wfm.set_scenario_status(0, sc_id, 1, 'IDLE', 0)
                logger.error("Failed to save product's raster file")
                return HttpResponseRedirect('http://127.0.0.1:'+port+'/scenario/overview/')

            scripts = get_scenario_script_paths(scenario)
            if len(scripts) > 0:
                # send request/task to work-flow-manager to run script
                current_task = work_flow_manager.WorkerTask(
                    {"scenario_id": sc_id,
                    "ncn_id"   : sc_ncn_id,
                    "task_type": "INGEST_LOCAL_PROD",
                    "scripts"  : scripts,
                    "cat_registration"  : scenario.cat_registration,
                     "s2_preprocess"    : scenario.s2_preprocess,
                    "dir_path" : full_directory_name,
                    "metadata" : request.FILES['metadataFile']._get_name(),
                    "data"     : request.FILES['rasterFile']._get_name(),
                    })

                wfm.put_task_to_queue(current_task)
                logger.info('Operation: ingest scenario: id=%d name=%s' \
                        % (scenario.id,scenario.scenario_name))
            else:
                logger.error("NOT INGESTING: Scenario '%s' name=%s does not have scripts to ingest." \
                        % (scenario.ncn_id,scenario.scenario_name))
                wfm.set_scenario_status(0, sc_id, 1, 'IDLE', 0)

            return HttpResponseRedirect('http://127.0.0.1:'+port+'/scenario/overview/')
        else:
            wfm.set_scenario_status(0, sc_id, 1, 'IDLE', 0)
            logger.warning('The add product form has not been fully/correctly filled')
            return HttpResponseRedirect('http://127.0.0.1:'+port+'/scenario/overview/')
    else:
        form = forms.AddLocalProductForm()
        variables = RequestContext(request, {
                'form'     :form,
                'home_page':IE_HOME_PAGE})
        return render_to_response('addLocalProduct.html', variables)

def getAddProductResult(request, sc_id, error):

    response_data = []
    response_data.append({'sc_id'          :'%s' % sc_id})
#       'metadata_file'  :'%s' % request.FILES['metadataFile']._get_name(),
#       'raster_file'    :'%s' % request.FILES['rasterFile']._get_name()

    if error != None:
        response_data.append({'status': 1, 'error' : '%s' % error })
    else :
        response_data.append({'status': 0 })

    return "addLocalProductResult", response_data

def add_local_product_core(request, ncn_id, template, aftersave=None):
    # add local product to the related scenario

    msg = "The new product has been successfully added"
    if request.method == 'POST':

        wfm = work_flow_manager.WorkFlowManager.Instance()
        scenario = models.Scenario.objects.get(ncn_id=ncn_id)
        sc_id  = scenario.id

        if wfm.lock_scenario(sc_id):

            try:

              form = forms.AddLocalProductForm(request.POST, request.FILES)
              if form.is_valid() and form.is_multipart():

                  full_directory_name, directory_name = create_dl_dir(ncn_id+"_")
                  full_directory_name = full_directory_name.encode('ascii','ignore')
                  logger.info("Product directory name:" + full_directory_name)

                  if 'metadataFile' in request.FILES:
                      saveFile(request.FILES['metadataFile'], full_directory_name)
                      meta = request.FILES['metadataFile']._get_name().encode('ascii','ignore')

                  else:
                      meta = None

                  saveFile(request.FILES['rasterFile'], full_directory_name)
                  data = request.FILES['rasterFile']._get_name().encode('ascii','ignore')

                  scripts = []
                  if scenario.oda_server_ingest != 0:
                      scripts.append(os.path.join(IE_SCRIPTS_DIR, IE_DEFAULT_ADDPROD_SCRIPT))
                  if len(scripts) == 0:
                      logger.warning("Scenario '%s' name='%s' does not have scripts to ingest, proceeding regardless." \
                          % (ncn_id,scenario.scenario_name))

                  # send request/task to work-flow-manager to run script
                  current_task = work_flow_manager.WorkerTask(
                      {"scenario_id": sc_id,
                       "ncn_id"   : ncn_id,
                       "task_type": "INGEST_LOCAL_PROD",
                       "scripts"  : scripts,
                       "cat_registration" : scenario.cat_registration,
                       "s2_preprocess"    : scenario.s2_preprocess,
                       "dir_path" : full_directory_name,
                       "metadata" : meta,
                       "data"     : data,
                       })

                  wfm.put_task_to_queue(current_task)
                  logger.info("Local ingest scenario - submitted to queue: " +
                              "id=%d, ncn_id='%s', name='%s'" \
                            % (sc_id, ncn_id, scenario.scenario_name))
              else:
                  logger.warning('The add product form has not been fully/correctly filled')

            except Exception, e:
                logger.error("Failed to add local product to Scenario '%s' name=%s" \
                                 % (ncn_id, scenario.scenario_name) + \
                                 'exception = ' + `e`)
            finally:
                wfm.set_scenario_status(0, sc_id, 1, 'IDLE', 0)

            if aftersave:
                variables = RequestContext(request, {})
                return render_to_response(aftersave,variables)
            else:
                port = request.META['SERVER_PORT']
                return HttpResponseRedirect(
                    'http://127.0.0.1:' + port + '/scenario/overview/')

        else:
            logger.warning("Ingest Scenario refused: " +
                           "Scenario '%s' name=%s is busy." % \
                               (ncn_id, scenario.scenario_name))
    else:
        form = forms.AddLocalProductForm()
        variables = RequestContext(request, {
                'form'     :form,
                'home_page':IE_HOME_PAGE})
        return render_to_response(template, variables)

    variables = RequestContext(request, {
            'form'     :form,
            'home_page':IE_HOME_PAGE})
    return render_to_response(template, variables)


def addLocalProduct(request, ncn_id):
    return add_local_product_core(
        request,
        ncn_id,
        'addLocalProduct.html')

def odaAddLocalProduct(request, ncn_id):
    oda_init(request)
    return add_local_product_core(
        request,
        ncn_id,
        'odaAddLocalProduct.html',
        'uploadedPage.html')


def show_log_core(request, template):
    variables = RequestContext(
        request,
        {'home_page':''})
    return render_to_response( template, variables)

def edit_scenario_core(request, scenario_id, template, aftersave):
    """ used for both add_scneario and edit_scenario
        for add_scenario the scenario_id is None
    """

    scenario = None
    editing  = False
    if scenario_id:
        editing = True
        scenario = models.Scenario.objects.get(id=int(scenario_id))

    if request.method == 'POST':
        # if POST, the user has now edited the form.
        form = forms.ScenarioForm(request.POST)

        if form.is_valid() and ncn_is_valid(form, scenario_id):
            scenario = form.save(commit=False)
            if editing:
                scenario.id = int(scenario_id)
            scenario.user = request.user
            if not scenario.default_priority:
                scenario.default_priority = 100
            scenario.save()

            # scenario scripts, eoids, and extras
            handle_uploaded_scripts(request,scenario)
            handle_eoids(request,scenario)
            handle_extras(request,scenario)

            if not editing:
                init_status(scenario)

            logger.info('Operation: add or edit scenario: id=%d name=%s' \
                            % (scenario.id, scenario.scenario_name),
                        extra={'user':request.user})

            if aftersave:
                variables = RequestContext(request, {})
                return render_to_response(aftersave,variables)
            else:
                port = request.META['SERVER_PORT']
                return HttpResponseRedirect(
                    'http://127.0.0.1:' + port + '/scenario/overview/')

        else:
            logger.warn("Scenario form is not valid",extra={'user':request.user})
            # fall through to display the form again,
            # with errors showing automatically, but save the
            # users' extras input
            handle_extras(request,scenario)

    else:
        form = forms.ScenarioForm()
        if editing:
            # initialize values of form 
            for field in form.fields:
                form.fields[field].initial = getattr(scenario,field)   

    scripts = []
    eoids   = []
    extras  = []
    if editing:
        # load scripts, eoids, and extras
        for field in form.fields:
            form.fields[field].initial = getattr(scenario,field)   
        scripts = scenario.script_set.all()
        eoids   = scenario.eoid_set.all()
        extras  = scenario.extraconditions_set.all()

    variables = RequestContext(
        request,
        {'form':form,
         'scripts':scripts,
         'eoids_in':eoids,
         'extras_in':extras,
         'sequence':"",
         'jqueryui_offlineurl': JQUERYUI_OFFLINEURL,
         'home_page':IE_HOME_PAGE})
    return render_to_response(template, variables)

def addScenario(request):
    return edit_scenario_core(
        request,
        None,
        'editScenario.html',
        None)


def odaAddScenario(request):
    oda_init(request)
    return edit_scenario_core(
        request,
        None,
        'odaEditScenario.html',
        'savedPage.html')

def editScenario(request, ncn_id):
    
    return edit_scenario_core(
        request,
        get_scenario_id(ncn_id),
        'editScenario.html',
        None)


def odaEditScenario(request, ncn_id):
    oda_init(request)
    return edit_scenario_core(
        request,
        get_scenario_id(ncn_id),
        'odaEditScenario.html',
        'savedPage.html')

def odaShowlog(request):
    oda_init(request)
    return show_log_core(request, 'odaShowLog.html')


def deleteScenario(request, ncn_id):
    # delete form and connection to related scripts and user,
    # and also deletes the whole scenario
    scenario = models.Scenario.objects.get(ncn_id=ncn_id)
    scenario_id = scenario.id
    scripts = scenario.script_set.all()

    # send request/task to work-flow-manager to run delete script
    wfm = work_flow_manager.WorkFlowManager.Instance()
    user = request.user
    del_script = models.UserScript.objects.filter(
        script_name__startswith="deleteScenario-",user_id__exact=user.id)[0]
    current_task = work_flow_manager.WorkerTask(
        {"scenario_id":scenario_id,
         "task_type":"DELETE_SCENARIO",
         "scripts":["%s/%s" % (MEDIA_ROOT,del_script.script_file)]
         })
    wfm.put_task_to_queue(current_task)

    # TODO problem: wfm dele is running meanwhile django deletes scenario and
    #   scripts ... django should wait to delete them ...

    # delete scenario, scripts and scenario-status from db (scenario-status is bounded to scenario)
    delete_scripts(scripts)
    scenario.delete()

    logger.info('Operation: delete scenario: id=%d name=%s' % (scenario.id,scenario.scenario_name) ,extra={'user':request.user})

    port = request.META['SERVER_PORT']
    return HttpResponseRedirect(
        'http://127.0.0.1:'+port+'/scenario/overview/')

def configuration_page(request):
    user = request.user
    variables = RequestContext(
        request,
        {"user":user,
         'home_page':IE_HOME_PAGE})
    return render_to_response('configuration.html',variables)

##################################################################
# Non-interactive intefaces


def getListScenarios(request):
    response_data = []
    scenarios = models.Scenario.objects.all()
    for s in scenarios:
        response_data.append(
            {'ncn_id':'%s' % s.ncn_id,
             'name': '%s' % s.scenario_name,
             'description':'%s' % s.scenario_description})
    return "scenarios", response_data

def getAjaxScenariosList(request):
    response_data = []
    scenarios = models.Scenario.objects.all()
    for s in scenarios:
        auto_ingest = 0;
        if s.repeat_interval > 0 : auto_ingest = 1;
        ss = get_or_create_scenario_status(s)
        response_data.append(
            {
                'id'                  : '%s' % s.id,
                'ncn_id'              : '%s' % s.ncn_id,
                'auto_ingest'         : auto_ingest,
                'scenario_name'       : '%s' % s.scenario_name,
                'scenario_description': '%s' % s.scenario_description,
                'st_isav':  ss.is_available,
                'st_st'  :  ss.status,
                'st_done':  ss.done
            })
    return "scenarios", response_data

def getScenario(request,args):
    response_data = {}
    scenario = models.Scenario.objects.get(ncn_id=args[0])
    response_data = models.scenario_dict(scenario)
    return "scenario", response_data

def set_sc_bbox(scenario, bb):
    scenario.bb_lc_long = bb["lc"][0]
    scenario.bb_lc_lat  = bb["lc"][1]
    scenario.bb_uc_long = bb["uc"][0]
    scenario.bb_uc_lat  = bb["uc"][1]

def un_unistr(s):
    if isinstance(s, basestring):
        return s.encode('ascii','ignore')
    else:
        return s

def set_sc_dates(scenario, data):
    for d in ("from_date", "to_date", "starting_date"):
        if d in data:
            val = un_unistr(data[d])
            exec "scenario."+d+"=models.date_from_iso8601('"+val+"')"
    
def set_sc_other(scenario, data):
    eval_str = ""
    all_keys = models.EXT_PUT_SCENARIO_KEYS + models.EXT_GET_SCENARIO_STRINGS
    for a in all_keys:
        if a in data:
            val = un_unistr(data[a])
            eval_str += "scenario."+a+"="+`val`+"\n"
    exec eval_str
        
    scenario.save()

    if 'extraconditions' in data:
        for e in data['extraconditions']:
            extra = models.ExtraConditions()
            extra.xpath    = un_unistr(e[0].encode)
            extra.text     = un_unistr(e[1].encode)
            extra.scenario = scenario
            extra.save()

    if "dssids" in data:
        for e in data['dssids']:
            dssid = models.Eoid()
            dssid.selected = True
            dssid.eoid_val = un_unistr(e)
            dssid.scenario = scenario
            dssid.save()
            
 
def update_core(data):
    # expected to be called from within a try block
    ncn_id = data['ncn_id']
    scenario = models.Scenario.objects.get(ncn_id=ncn_id)
    # extra conditions are deleted, they must be re-sent
    # if the user wishes to keep them.
    extras = scenario.extraconditions_set.all()
    for e in extras:
        e.delete()
    extras = None
    if'aoi_bbox' in data:
        set_sc_bbox(scenario, data["aoi_bbox"])
    set_sc_dates(scenario, data)
    set_sc_other(scenario, data)
    logger.info("ManageScenario: updated scenario " + scenario.ncn_id)
    return scenario.ncn_id

def new_sc_core(data):
    # expected to be called from within a try block
    
    ncn_id = None
    if 'ncn_id' in data and data['ncn_id']:
        ncn_id = data['ncn_id'].encode('ascii','ignore')
        try:
            scenario = models.Scenario.objects.get(ncn_id=ncn_id)
        except  models.Scenario.DoesNotExist:
            pass
        else:
            raise ManageScenarioError("Non-unique ncn-id: "+ncn_id)
    else:
        ncn_id = models.make_ncname(SC_NCN_ID_BASE)

    scenario = models.Scenario()
    scenario.ncn_id = ncn_id
    scenario.user = models.User.objects.get(username='drtest')
    if 'aoi_bbox' in data:
        set_sc_bbox(scenario, data["aoi_bbox"])
    else:
        raise ManageScenarioError("'aoi_bbox' is mandatory")

    if not "aoi_type"  in data:
        scenario.aoi_type = "BB"
    if not "default_priority" in data:
        scenario.default_priority = 100
    if not "starting_date" in data:
        data["starting_date"] = "2014-01-01T11:22:33"
    if not "repeat_interval" in data:
        scenario.repeat_interval = 0
    if not "oda_server_ingest" in data:
        scenario.oda_server_ingest = 1
    if not "tar_result" in data:
        scenario.tar_result = 0
    if not "cat_registration" in data:
        scenario.cat_registration = 0
    if not "coastline_check" in data:
        scenario.coastline_check = 0

# TODO set preprocessA/B/C flags CONTINUE HERE
    scenario.preprocessA = 0
    scenario.preprocessB = 0
    scenario.preprocessC = 0

    set_sc_dates(scenario, data)
    set_sc_other(scenario, data)
    
    logger.info("ManageScenario: added new scenario " + scenario.ncn_id)
    return scenario.ncn_id

def updateOrNew(str_data, op):
    response_data = {}
    data = None
    try:
        data = json.loads(str_data.encode('ascii','ignore'))
    except Exception as e:
        response_data['status'] = 1
        response_data['error']  = "Malformed json data in POST: " + `e`
    else:
        wfm = work_flow_manager.WorkFlowManager.Instance()
        wfm.lock_db()
        try:
            ncn_id = op(data)
            response_data['status'] = 0
            response_data['ncn_id'] = ncn_id
        except Exception as e:
            response_data['status'] = 1
            response_data['error']  = `e`
        finally:
            wfm.release_db()

    return response_data

    
def updateScenario(data, request_meta):
    return updateOrNew(data, update_core)

def newScenario(data, request_meta):
    return updateOrNew(data, new_sc_core)


def getAddStatus(request,args):
    # Possible Error values:
    #     processing
    #     failed
    #     success
    #     idError
    #

    response_data = {}

    iid = int(args[0])
    pi = None
    try:
        pi = models.ProductInfo.objects.get(id__exact=iid)
        response_data['status'] = pi.info_status

        if pi.info_error:
            response_data["errorString"] = pi.info_error

        if pi.new_product_id:
            response_data["productId"] = pi.new_product_id

        if pi.product_url:
            response_data["url"] = pi.product_url

    except models.ProductInfo.DoesNotExist:
        response_data['status'] = "idError"
        response_data["errorString"] = "Id not found."

    return response_data

@csrf_exempt
def odaReset_core(request, args):
    oda_init(request)
    ncn_id = args[0].encode('ascii','ignore')
    return reset_scenario_core(ncn_id=ncn_id)

@csrf_exempt
def odaDel_core( request, args ):
    oda_init(request)
    ncn_id = args[0].encode('ascii','ignore')
    return delete_scenario_core(ncn_id=ncn_id)

@csrf_exempt
def odaStop_core( request, args ):
    oda_init(request)
    ncn_id = args[0].encode('ascii','ignore')
    scenario = models.Scenario.objects.get(ncn_id=ncn_id)
    stop_ingestion_core(scenario_id=scenario.id)
    return {'status':0}


@csrf_exempt
def odaIngest_core( request, args ):
    oda_init(request)
    response_data = {}
    ncn_id = args[0].encode('ascii','ignore')
    return ingest_scenario_core(ncn_id=ncn_id)


def get_request_json(func,
                     request,
                     args=None,
                     string_error=False,
                     wrapper=True):
    """ if wrapper is True, func is expected to return a tuple:
        (key_string, data)
        where string is the keyword in the final json response
        sent to the requestor, and data is its value.
        The response will then be json structure like this:
          { "status" : 0 , key_string : data }
        If wrapper is False, the response is returned without
        the "status" json wrapper, and need not be tuple then.
        args is a tuple passed on to the func
    """
    response_data = {}
    STATUS_KEY = 'status'
    ERROR_KEY  = 'error'

    oda_init(request)

    if string_error:
        failure_status = 'failed'
        success_status = 'errorString'
    else:
        failure_status = 1
        success_status = 0

    if request.method == 'GET' or request.method == 'POST':
        try:
            if args:
                op_response = func(request, args=args)
            else:
                op_response = func(request)
            if wrapper:
                response_data[STATUS_KEY] = success_status
                response_data[op_response[0]] = op_response[1]
            else:
                response_data = op_response
        except Exception as e:
            logger.error('Error in get_request_json(' + \
                             "func=" + `func`+', args=' + `args`+\
                             '): ' + \
                             `e`)
            response_data[STATUS_KEY] = failure_status
            response_data[ERROR_KEY]  = "%s" % e
    else:
        # method was not GET or POST
        response_data[STATUS_KEY] = failure_status
        response_data[ERROR_KEY]  = "Request method is not GET or POST."

    ret = HttpResponse(
        json.dumps(response_data),
        content_type="application/json")
    ret["Access-Control-Allow-Origin"] = "*"
    return ret

@csrf_exempt
def getListScenarios_operation(request):
    # expect a GET request
    return get_request_json(getListScenarios, request)

@csrf_exempt
def addOdaLocalProduct_operation(request, ncn_id):
    # expect a GET request
    return get_request_json(addOdaLocalProduct, request, (ncn_id,) )

@csrf_exempt
def getAjaxScenariosList_operation(request):
    # expect a GET request
    return get_request_json(getAjaxScenariosList, request)

@csrf_exempt
def getScenario_operation(request,ncn_id):
    return get_request_json(getScenario, request, args=(ncn_id,))

@csrf_exempt
def updateScenario_operation(request):
    return do_post_operation(updateScenario, request)

@csrf_exempt
def newScenario_operation(request):
    return do_post_operation(newScenario, request)

@csrf_exempt
def addProduct_operation(request):
    return do_post_operation(add_product_submit, request)

@csrf_exempt
def getAddStatus_operation(request, op_id):
    return get_request_json(getAddStatus,
                            request,
                            args=(op_id,),
                            string_error=True,
                            wrapper=False)


@csrf_exempt
def darResponse(request,seq_id):
    seq_id = seq_id.encode('ascii','ignore')
    if request.method == 'GET':
        logger.info("Request to retrieve DAR from" + \
                        `request.META['REMOTE_ADDR']` +\
                        ", id="+`seq_id`)
        dar = dmcontroller.get_next_dar(seq_id)
        if None == dar:
            logger.error("No dar!")
            server_error(request)
        else:
            return HttpResponse(
                dar,
                content_type="application/xml")
    else:
        logger.error("Unxexpected POST request on darResponse url,\n" + \
                         `request.META['SERVER_PORT']`)
        raise Http404


@csrf_exempt
def dmDARStatus(request):
    response_data = {}
    if request.method == 'GET':
        dm_url = dmcontroller._dm_url
        url = dm_url+DM_DAR_STATUS_COMMAND
        try:
            response_data = json.loads(read_from_url(url))
        except URLError as e:
            return HttpResponse(
                "Cannot connect to Download Manager: " + `e`,
                content_type="text/plain")
            
        response_str = json.dumps(response_data,indent=4)
        return HttpResponse(response_str,content_type="text/plain")
    else:
        logger.error("Unxexpected POST request on dmDARStatus url,\n" + \
                         `request.META['SERVER_PORT']`)
        raise Http404

@csrf_exempt
def updateMD_operation(request):
    return do_post_operation(updateMetaData, request)

@csrf_exempt
def odaDeleteScenario(request, ncn_id):
    return get_request_json(odaDel_core, request, (ncn_id,), wrapper=False)

@csrf_exempt
def odaResetScenario(request, ncn_id):
    return get_request_json(odaReset_core, request, (ncn_id,), wrapper=False)

@csrf_exempt
def odaStopIngestion(request, ncn_id):
    return get_request_json(odaStop_core, request, (ncn_id,), wrapper=False)

@csrf_exempt
def mngStopIngestion(request, ncn_id):
    return get_request_json(odaStop_core, request, (ncn_id,), wrapper=False)

@csrf_exempt
def odaIngest(request, ncn_id):
    return get_request_json(odaIngest_core, request, (ncn_id,), wrapper=False )

