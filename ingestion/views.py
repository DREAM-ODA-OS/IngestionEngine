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
from urllib2 import URLError

import os
import logging
import datetime
import json

import models
import forms
import work_flow_manager

from settings import set_autoLogin, \
    IE_DEBUG, IE_HOME_PAGE, IE_AUTO_LOGIN, \
    MEDIA_ROOT, JQUERYUI_OFFLINEURL, AUTHENTICATION_BACKENDS
from utils import read_from_url
from dm_control import DownloadManagerController, DM_DAR_STATUS_COMMAND
from uqmd import updateMetaData
from add_product import add_product_submit

IE_DEFAULT_USER = r'dreamer'
IE_DEFAULT_PASS = r'1234'

dmcontroller = DownloadManagerController.Instance()

def do_post_operation(op, request):
    # must be a POST request
    response_data = {}

    if request.method == 'POST':
        response_data = op(request.body)
        return HttpResponse(
            json.dumps(response_data),
            content_type="application/json")

    else:
        logger = logging.getLogger('dream.file_logger')
        logger.error("Unxexpected GET request on url "+`request.path_info`)
        raise Http404

def auto_login(request):
    if not IE_AUTO_LOGIN:
        return

    logger = logging.getLogger('dream.file_logger')
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
        try:
            scenario_status.append(s.scenariostatus)
        except models.ScenarioStatus.DoesNotExist:
            print 'adding '+`s.ncn_id`
            sstat = models.ScenarioStatus(
                scenario=s,
                is_available=1,
                status='IDLE',
                done=0.0)
            sstat.save()
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
            logger = logging.getLogger('dream.file_logger')
            logger.error("Deletion error: " + `e`, extra={'user':"drtest"})

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
            logger = logging.getLogger('dream.file_logger')
            logger.error("Upload error: " + `e`, extra={'user':request.user})
        finally:
            destination.close()
        script = models.Script()
        script.script_name = names[i]
        script.script_path = file_path
        script.scenario = scenario
        script.save()
        i = i + 1   

def addScenario(request):
    # add scenario and related scripts 
    logger = logging.getLogger('dream.file_logger')
    if request.method == 'POST':
        form = forms.ScenarioForm(request.POST)
        if form.is_valid():
            if IE_DEBUG > 2:
                logger.debug( "Scenario form is valid")
            scenario = form.save(commit=False)
            scenario.user = request.user
            scenario.save()
            # scenario scripts
            handle_uploaded_scripts(request,scenario)
            # scenario status
            scenario_status = models.ScenarioStatus()
            scenario_status.scenario = scenario
            scenario_status.is_available = 1
            scenario_status.status = "IDLE"
            scenario_status.done = 0
            scenario_status.save()

            logger.info('Operation: add scenario: id=%d name=%s' % \
                            (scenario.id,scenario.scenario_name),
                        extra={'user':request.user})

            port = request.META['SERVER_PORT']
            return HttpResponseRedirect(
                'http://127.0.0.1:'+port+'/scenario/overview/')
        else:
            if IE_DEBUG > 0:
                logger.debug( "Scenario form is not valid",extra={'user':"drtest"})
            return render_to_response(
                'editScenario.html',
                {'form':form,
                 'home_page':IE_HOME_PAGE,
                 'status':"Error on form."})
    else:
        form = forms.ScenarioForm()
    variables = RequestContext(request,
                               {'form':form,
                                'scripts':[],
                                'sequence':"",
                                'home_page':IE_HOME_PAGE})
    return render_to_response('editScenario.html',variables)

def editScenario(request,scenario_id):
    # edit scenario and its relationship to user and scripts 
    # first, scenario and relationship are deleted and then added as in 
    logger = logging.getLogger('dream.file_logger')
    scenario = models.Scenario.objects.get(id=int(scenario_id))
    if request.method == 'POST':
        form = forms.ScenarioForm(request.POST)
        if form.is_valid():
            # delete form and connection to related scripts and user
            scenario = form.save(commit=False)
            scenario.id = int(scenario_id)
            scenario.user = request.user
            scenario.save()
            logger.info('Operation: edit scenario: id=%d name=%s' \
                            % (scenario.id,scenario.scenario_name),
                        extra={'user':request.user})

            if IE_DEBUG > 1:
                print "Scenario: %s %s" % (str(scenario.id),scenario.scenario_name)
            handle_uploaded_scripts(request,scenario)
            port = request.META['SERVER_PORT']
            return HttpResponseRedirect(
                'http://127.0.0.1:' + port + '/scenario/overview/')
        else:
            logger.warn("Scenario form is not valid",extra={'user':request.user})
    else:
        form = forms.ScenarioForm()
        # initialize values of form
        for field in form.fields:
            form.fields[field].initial = getattr(scenario,field)   
    # load scripts
    scripts = scenario.script_set.all()
    variables = RequestContext(
        request,
        {'form':form,
         'scripts':scripts,
         'sequence':"",
         'home_page':IE_HOME_PAGE})
    return render_to_response('editScenario.html',variables)

def deleteScenario(request,scenario_id):
    # delete form and connection to related scripts and user
    # it deletes also whole scenario - if scenario is connected to other 
    scenario = models.Scenario.objects.get(id=int(scenario_id))
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

    logger = logging.getLogger('dream.file_logger')
    logger.info('Operation: delete scenario: id=%d name=%s' % (scenario.id,scenario.scenario_name) ,extra={'user':request.user})

    port = request.META['SERVER_PORT']
    return HttpResponseRedirect(
        'http://127.0.0.1:'+port+'/scenario/overview/')

def configuration_page(request):
    if request.method == 'POST':

        for i in request.FILES:
            print i

        print "L: %d" % len(request.FILES)

        form = forms.UserScriptForm(request.POST,request.FILES)
        if form.is_valid():
            # delete form and connection to related scripts and user
            m = form.save(commit=False)
            m.user = request.user

            if "button_submit_1" in request.POST: # name of button in template
                print "AddProduct Form  is valid"
                m.script_name = "addProduct-script"
                # delete old scripts from db and media/scripts
                for old_script in models.UserScript.objects.filter(user_id__exact=2):
                    if old_script.script_name=="addProduct-script":
                        old_script.delete()
                        # TODO exception should be implemented
                        os.remove("%s/%s" % \
                                      (MEDIA_ROOT,old_script.script_file)) 

            elif "button_submit_2" in request.POST: # name of button in template
                print "Delete Form  is valid"
                m.script_name = "deleteScenario-script"
                # delete old scripts from db and media/scripts
                for old_script in models.UserScript.objects.filter(user_id__exact=2):
                    if old_script.script_name=="deleteScenario-script":
                        old_script.delete()
                        # TODO exception should be implemented
                        os.remove("%s/%s" % \
                                      (MEDIA_ROOT,old_script.script_file)) 
            # save UserScript to /<project>/media/{(user.id)_(script_name)}
            m.save() 
            port = request.META['SERVER_PORT']
            return HttpResponseRedirect(
                'http://127.0.0.1:'+port+'/account/configuration/')
        else:
            print "Form is not valid"

    user = request.user
    del_script = None
    pro_script = None
    for del_item in models.UserScript.objects.filter(script_name__exact="deleteScenario-script",user_id=user.id):
        print "query: %s" % del_item.script_name
        del_script = del_item
    for pro_item in models.UserScript.objects.filter(script_name__exact="addProduct-script",user_id=user.id):
        pro_script = pro_item

    variables = RequestContext(
        request,
        {"product_script":pro_script,
         "delete_script":del_script,
         "user":user,
         'home_page':IE_HOME_PAGE})
    return render_to_response('configuration.html',variables)

##################################################################
# Non-interactive intefaces


def getListScnerios(request):
    response_data = []
    scenarios = models.Scenario.objects.all()
    for s in scenarios:
        response_data.append(
            {'id':'%s' % s.ncn_id,
             'name': '%s' % s.scenario_name,
             'decription':'%s' % s.scenario_description})
    return "scenarios", response_data

def getScenario(request,args):
    response_data = {}
    scenario = models.Scenario.objects.get(ncn_id=args[0])
    response_data = models.scenario_dict(scenario)
    return "scenario", response_data

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


def get_request_json(func, request,
                     args=None,
                     string_error=False,
                     wrapper=True):
    """ if wrapper is True, func is expected to return a tuple:
        (key_string, data)
        where string is the keyword in the final json response
        sent to the requestor, and data is its value.
        The response will then be json structure like this:
          { "status" : 0 , key_string : data }
        If wrapper is True, the response is returned without
        the "status" json wrapper, and need not be tuple then.
        args is a tuple passed on to the func
    """
    response_data = {}

    if string_error:
        failure_status = 'failed'
        success_status = 'success'
    else:
        failure_status = 1
        success_status = 0

    if request.method == 'GET':
        try:
            if args:
                op_response = func(request, args=args)
            else:
                op_response = func(request)
            if wrapper:
                response_data['status'] = success_status
                response_data[op_response[0]] = op_response[1]
            else:
                response_data = op_response
        except Exception as e:
            response_data['status'] = failure_status
            response_data['errorString'] = "%s" % e
    else:
        # method was POST
        response_data['status'] = failure_status
        response_data['errorString'] = "Request method is not GET."
    return HttpResponse(
        json.dumps(response_data),
        content_type="application/json")


@csrf_exempt
def getListScenarios_operation(request):
    # expect a GET request
    return get_request_json(getListScnerios, request)


@csrf_exempt
def getScenario_operation(request,ncn_id):
    return get_request_json(getScenario, request, args=(ncn_id,))


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
    logger = logging.getLogger('dream.file_logger')
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
        logger = logging.getLogger('dream.file_logger')
        logger.error("Unxexpected POST request on dmDARStatus url,\n" + \
                         `request.META['SERVER_PORT']`)
        raise Http404

@csrf_exempt
def updateMD_operation(request):
    return do_post_operation(updateMetaData, request)
