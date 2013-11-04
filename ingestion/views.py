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

import json

import models
import forms
import settings
import os
import logging
import datetime
from django.utils.timezone import utc
from django.contrib.auth import authenticate, login

import work_flow_manager


def main_page(request):
    logger = logging.getLogger('dream.file_logger')
    if settings.IE_AUTO_LOGIN and \
            (not request.user.is_authenticated()) and request.path == '/':
        logger.info(" ** req="+`request.path`,extra={'user':"drtest"})
        new_user = authenticate(username=r'drtest', password=r'1234')
        if None == new_user:
            logger.warn('Cannot log in user drtest',extra={'user':"drtest"})
        else:
            new_user.backend=settings.AUTHENTICATION_BACKENDS[0]
            login(request, new_user)
            logger.info('Auto-logged-in.',extra={'user':"drtest"})
    return render_to_response('base.html',{'user':request.user})

def logout_page(request):
    settings.IE_AUTO_LOGIN = False
    logout(request)
    return HttpResponseRedirect('/')


def overviewScenario(request):
    # retrieve scenarios for current user
    user = request.user
    scenarios = user.scenario_set.all()
    scenario_status = []
    for s in scenarios:
        scenario_status.append(s.scenariostatus)
    variables = RequestContext(request,{'scenarios':scenarios,'scenario_status':scenario_status})

    return render_to_response('overviewScenario.html',variables)


def delete_scripts(scripts):
    for s in scripts:
        try:
            os.remove(s.script_path) # delete script related to current scenario
            script = models.Script.objects.get(script_path=s.script_path)
            script.delete()
        except Exception as e:
            print e

def handle_uploaded_scripts(request,scenario):
    if len(request.FILES)==0:
        return
    scripts = scenario.script_set.all()
    delete_scripts(scripts)
    names = request.POST.getlist('script_name')
    i = 0
    for filename in request.FILES: # files is dictionary
        f = request.FILES[filename] 
        file_path = "%s/scripts/%s_%s_%d" % (settings.MEDIA_ROOT,str(request.user.id),str(scenario.id),i)
        try:
            destination = open(file_path,'w')
            if f.multiple_chunks:
                for chunk in f.chunks():
                    destination.write(chunk)
            else:
                destination.write(f.read())
        except Exception as e:
            print "Exception %s" % e
            #break
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
    if request.method == 'POST':
        form = forms.ScenarioForm(request.POST)
        if form.is_valid():
            print "Scenario form is valid"
            scenario = form.save(commit=False)
            scenario.user = request.user
            scenario.save()
            # scenario scripts
            handle_uploaded_scripts(request,scenario)
            # scenario status
            scenario_status = models.ScenarioStatus()
            scenario_status.scenario = scenario
            scenario_status.is_available = 1
            scenario_status.status = "FREE"
            scenario_status.done = 0
            scenario_status.save()

            # use logging
            logger = logging.getLogger('dream.file_logger')
            logger.info('Operation: add scenario: id=%d name=%s' % (scenario.id,scenario.scenario_name) ,extra={'user':request.user})


            return HttpResponseRedirect('http://127.0.0.1:8000/scenario/overview/')
        else:
            print "Scenario form is not valid"
            return render_to_response('editScenario.html',{'form':form})
    else:
        form = forms.ScenarioForm()
    variables = RequestContext(request,{'form':form,'scripts':[],"sequence":""})
    return render_to_response('editScenario.html',variables)

def editScenario(request,scenario_id):
    # edit scenario and its relationship to user and scripts 
    # first, scenario and relationship are deleted and then added as in 
    scenario = models.Scenario.objects.get(id=int(scenario_id))
    if request.method == 'POST':
        form = forms.ScenarioForm(request.POST)
        if form.is_valid():
            # delete form and connection to related scripts and user
            scenario = form.save(commit=False)
            scenario.id = int(scenario_id)
            scenario.user = request.user
            scenario.save()

            # use logging
            logger = logging.getLogger('dream.file_logger')
            logger.info('Operation: edit scenario: id=%d name=%s' % (scenario.id,scenario.scenario_name) ,extra={'user':request.user})

            print "Scenario: %s %s" % (str(scenario.id),scenario.scenario_name)
            handle_uploaded_scripts(request,scenario)
            return HttpResponseRedirect('http://127.0.0.1:8000/scenario/overview/')  
        else:
            print "Scenario form is not valid"
    else:
        form = forms.ScenarioForm()
        # initialize values of form
        for field in form.fields:
            form.fields[field].initial = getattr(scenario,field)   
    # load scripts
    scripts = scenario.script_set.all()
    variables = RequestContext(request,{'form':form,'scripts':scripts,"sequence":""})
    return render_to_response('editScenario.html',variables)

def deleteScenario(request,scenario_id):
    # delete form and connection to related scripts and user
    # it deletes also whole scenario - if scenario is connected to other 
    scenario = models.Scenario.objects.get(id=int(scenario_id))
    scripts = scenario.script_set.all()

    # send request/task to work-flow-manager to run delete script
    wfm = work_flow_manager.WorkFlowManager.Instance()
    user = request.user
    del_script = models.UserScript.objects.filter(script_name__startswith="deleteScenario-",user_id__exact=user.id)[0]
    current_task = work_flow_manager.WorkerTask({"scenario_id":scenario_id,"task_type":"DELETING-SCENARIO","scripts":["%s/%s" % (settings.MEDIA_ROOT,del_script.script_file)]})
    wfm.put_task_to_queue(current_task)

    # problem: wfm deleting is running meanwhile django deletes scenario and scripts ... django should wait to delete them ...


    # delete scenario, scripts and scenario-status from db (scenario-status is bounded to scenario)
    delete_scripts(scripts)
    scenario.delete()

    # use logging
    logger = logging.getLogger('dream.file_logger')
    logger.info('Operation: delete scenario: id=%d name=%s' % (scenario.id,scenario.scenario_name) ,extra={'user':request.user})

    return HttpResponseRedirect('http://127.0.0.1:8000/scenario/overview/')

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
                        os.remove("%s/%s" % (settings.MEDIA_ROOT,old_script.script_file)) # exception should be implemented

            elif "button_submit_2" in request.POST: # name of button in template
                print "Delete Form  is valid"
                m.script_name = "deleteScenario-script"
                # delete old scripts from db and media/scripts
                for old_script in models.UserScript.objects.filter(user_id__exact=2):
                    if old_script.script_name=="deleteScenario-script":
                        old_script.delete()
                        os.remove("%s/%s" % (settings.MEDIA_ROOT,old_script.script_file)) # exception should be implemented

            m.save() # save UserScript to /<project>/media/{(user.id)_(script_name)}
            return HttpResponseRedirect('http://127.0.0.1:8000/account/configuration/')
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

    variables = RequestContext(request,{"product_script":pro_script,"delete_script":del_script,'user':user})
    return render_to_response('configuration.html',variables)


@csrf_exempt
def getListScenarios(request):
    # must be GET request
    response_data = []
    if request.method == 'GET':
        try:
            scenarios = models.Scenario.objects.all()
            for s in scenarios:
                response_data.append({'id':'%s' % s.id,'name': '%s' % s.scenario_name,'decription':'%s' % s.scenario_description})
        except Exception as e:
            response_data['status'] = 1
            response_data['errorString'] = "%s" % e
    else:
        # method POST
        response_data['status'] = 1
        response_data['errorString'] = "Request method is not GET."
    return HttpResponse(json.dumps(response_data), content_type="application/json")


@csrf_exempt
def getScenario(request,scenario_id):
    # must be GET method
    response_data = {}
    if request.method == 'GET':
        try:
            scenario = models.Scenario.objects.get(id=scenario_id)
            for s in scenario.__dict__:
                if not s.startswith("_"):
                    response_data[s] = "%s" % str(getattr(scenario,s))
        except Exception as e:
            response_data['status'] = 1
            response_data['errorString'] = "%s" % e
    else:
        # method POST
        response_data['status'] = 1
        response_data['errorString'] = "Request method is not GET."
    return HttpResponse(json.dumps(response_data), content_type="application/json")


@csrf_exempt
def addProduct(request):
    # must be POST request
    response_data = {}
    if request.method == 'POST':
        dataRef = request.POST['dataRef']
        username = request.POST['username']

        if dataRef: # metadata will be implemented later
            addProduct = models.ProductInfo()
            addProduct.info_date = datetime.datetime.utcnow().replace(tzinfo=utc)
            addProduct.info_status = "processing"
            addProduct.save()

            response_data['opId'] = str(addProduct.id)
            response_data['status'] = 0

            # process addProduct data by work_flow_manager
            wfm = work_flow_manager.WorkFlowManager.Instance()
            script = None
            try:
                user = User.objects.get(username=username)
                script = models.UserScript.objects.filter(script_name__exact="addProduct-script",user_id=user.id)
                if len(script)>0:
                    print "%s/%s" %(settings.MEDIA_ROOT,script[0].script_file)
                    current_task = work_flow_manager.WorkerTask({"task_type":"ADD-PRODUCT","addProductScript":"%s/%s" % (settings.MEDIA_ROOT,script[0].script_file),"dataRef":dataRef,"addProduct_id":addProduct.id})
                    wfm.put_task_to_queue(current_task)
                else:
                    response_data['status'] = 1
                    response_data['errorString'] = "User: %s doesn't have defined addProduct script." % username
            except Exception as e:
                response_data['status'] = 1
                response_data['errorString'] = "Error %s" % e
        else:
            response_data['status'] = 1
            response_data['errorString'] = "Missing input data."
    else:
        response_data['status'] = 1
        response_data['errorString'] = "Request is not POST."
    print response_data
    print datetime.datetime.utcnow().replace(tzinfo=utc)
    return HttpResponse(json.dumps(response_data), content_type="application/json")


def getStatus(request):
    # must be GET request
    response_data = {}
    if request.method == 'GET':

        if request.opId:
            pass


        else:
            response_data['status'] = "failed"
            response_data['errorString'] = "There is no addProduct id."


    else:
        response_data['status'] = "failed"
        response_data['errorString'] = "Request is not GET."

