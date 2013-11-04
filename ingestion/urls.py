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


urlpatterns = patterns('',

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),

    # site_media
    (r'^static/(?P<path>.*)$', 'django.views.static.serve',{'document_root': settings.STATIC_ROOT}),

    # site media
    (r'^media/(?P<path>.*)$', 'django.views.static.serve',{'document_root': settings.MEDIA_ROOT}),

    # login.html
    url(r'^account/login/',login, {'template_name': 'login.html'}),

    # addProduct script (JSON)
    url(r'^ingest/addProduct/addProduct',views.addProduct),

    # listScenarios
    url(r'^ingest/ManageScenario/listScenarios',views.getListScenarios),

    # getScenario
    url(r'^ingest/ManageScenario/getScenario/id=(?P<scenario_id>[1-9]{1,3})',views.getScenario),

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
    
    # deleteScenario
    url(r'^scenario/delete/(?P<scenario_id>[1-9]{1,3})',views.deleteScenario),
    
    # editScenarioForms.html
    url(r'^scenario/edit/(?P<scenario_id>[1-9]{1,3})',views.editScenario),
    
    # Main page
    url(r'',views.main_page),
    
    # editScenarioForms.html
    #url(r'^editScenarioForms/',views.editScenarioForms),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

)

urlpatterns += staticfiles_urlpatterns()
