############################################################
#  Project: DREAM
#  Module:  Task 5 ODA Ingestion Engine 
#  Author:  Vojtech Stefka  (CVC)
#
#    (c) 2013 Siemens Convergence Creators s.r.o., Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
#
############################################################

from django.conf.urls import patterns, include, url
import views,settings
from django.contrib.auth.views import login
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

# dajaxice (ajax in django)
from dajaxice.core import dajaxice_autodiscover, dajaxice_config
dajaxice_autodiscover()

from settings import IE_HOME_PAGE


urlpatterns = patterns('',

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),

    # static files
    (r'^static/(?P<path>.*)$', 'django.views.static.serve',{'document_root': settings.STATIC_ROOT}),

    # site media
    (r'^media/(?P<path>.*)$', 'django.views.static.serve',{'document_root': settings.MEDIA_ROOT}),

    # login.html
    url(r'^account/login/',login, {'template_name': 'login.html'}),

    # addProduct (JSON) and the associated getStatus
    url(r'^ingest/addProduct/addProduct',views.addProduct_operation),
    url(r'^ingest/AddProduct/addProduct',views.addProduct_operation),
    url(r'^ingest/addProduct/getStatus/id=(?P<op_id>.*)$',
        views.getAddStatus_operation),
    url(r'^ingest/AddProduct/getStatus/id=(?P<op_id>.*)$',
        views.getAddStatus_operation),

    # uqmd - updateQualityMetaData (JSON / mixed)
    # Implements the interface  IF-DREAM-O-UpdateQualityMD
    url(r'^ingest/uqmd/updateMD', views.updateMD_operation),

    # listScenarios
    url(r'^ingest/ManageScenario/listScenarios',views.getListScenarios_operation),

    # listScenarios for ajax / backend
    url(r'^ingest/ManageScenario/ajaxListScenarios',views.getAjaxScenariosList_operation),

    # getScenario
    url(r'^ingest/ManageScenario/getScenario/id=(?P<ncn_id>.*)$',views.getScenario_operation),

    # DM DAR status list, used mostly for development/debugging
    url(r'^ingest/dmDARStatus',views.dmDARStatus),

    # logout.html
    url(r'^account/logout/',views.logout_page),
     
    # configuration
    url(r'^account/configuration/',views.configuration_page),

    # dajaxice                       
    url(dajaxice_config.dajaxice_url,include('dajaxice.urls')),
      
    # overviewScenario.html
    url(r'^scenario/overview/',views.overviewScenario),
    
    # editScripts.html
    #url(r'^editScripts/',views.editScripts),
    
    # addScenario.html
    url(r'^scenario/add/$',views.addScenario),
    
    # addLocalProduct.html
    url(r'^ingest/addLocal/(?P<ncn_id>.*)$',views.addLocalProduct),
    
    # odaAddLocalProduct.html
    url(r'^ingest/odaAddLocal/(?P<ncn_id>.*)$',views.odaAddLocalProduct),

    # deleteScenario
    url(r'^scenario/delete/(?P<ncn_id>.*)',views.deleteScenario),
    
    # editScenarioForms.html
    url(r'^scenario/edit/(?P<ncn_id>.*)',views.editScenario),

    # Edit Scenario for the ODAClient
    url(r'^scenario/odaedit/(?P<ncn_id>.*)',views.odaEditScenario),

    # Add Scenario for the ODAClient
    url(r'^scenario/odaadd/',views.odaAddScenario),

    # Delete Scenario for the ODAClient
    url(r'^scenario/odadelete/(?P<ncn_id>.*)', views.odaDeleteScenario),

    # Start Ingestion for the ODAClient
    url(r'^ingestion/odaingest/(?P<ncn_id>.*)', views.odaIngest),
    
    # Stop Ingestion for the ODAClient
    url(r'^ingestion/odastop/(?P<ncn_id>.*)', views.odaStopIngestion),
    
    # dar response
    url(r'^ingest/darResponse/(?P<seq_id>.*)$',views.darResponse),
    
    # Main page
    # Skip showing the main page views.main_page,
    # and go straight to scenario overview instead
    url(r'^'+IE_HOME_PAGE,    views.overviewScenario),
    url(r'^'+IE_HOME_PAGE+'/',views.overviewScenario),
    
    # editScenarioForms.html
    #url(r'^editScenarioForms/',views.editScenarioForms),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

)

urlpatterns += staticfiles_urlpatterns()
