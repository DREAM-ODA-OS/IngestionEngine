############################################################
#  Project: DREAM
#  Module:  Task 5 ODA Ingestion Engine 
#  Author:  Vojtech Stefka  (CVC)
#  Contributor: Milan Novacek (CVC)
#
#    (c) 2013 Siemens Convergence Creators s.r.o., Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
#
############################################################

import models
import django.forms as forms
import datetime
from django.utils.timezone import utc
from settings import SC_NCN_ID_BASE

class ScriptForm(forms.Form):
    name = forms.CharField(max_length=20)
    file = forms.FileField()


class UserScriptForm(forms.ModelForm):
    class Meta:
        model = models.UserScript
        exclude = ['id','user','script_name']


class ScriptsForm(forms.ModelForm):
    class Meta:
        model = models.Script
        #exclude = ['id_script']

class AddLocalProductForm(forms.Form):
    metadataFile = forms.FileField()
    rasterFile = forms.FileField()

class ScenarioForm(forms.ModelForm):
   
    class Meta:
       model = models.Scenario
       exclude = ['user',
                  'aoi_file',
                  'dsrc_password',
                  'dsrc_login',
                  'aoi_poly_lat',
                  'aoi_poly_long']

    def clean_cloud_cover(self):
       data = self.cleaned_data['cloud_cover']
       if data<0 or data>100:
           raise forms.ValidationError(
               "Max Cloud Cover must be in the interval from 0 to 100.")
       return data

    def clean_view_angle(self):
        data = self.cleaned_data['view_angle']
        if not data>=0 and data<90:
            raise forms.ValidationError(
               "Max View Angle must be in the interval from 0 to 90.")
        return data

    def clean_starting_date(self):
       data = self.cleaned_data['starting_date']
       if data == None:
          data = datetime.datetime.utcnow()
       return data

    def clean_from_date(self):
        data = self.cleaned_data['from_date']
        # for time-zone aware dates use this one instead:
        #t1 = datetime.datetime(1990,1,1,0,0,0).replace(tzinfo=utc)
        t1 = datetime.datetime(1990,1,1,0,0,0)
        t2 = datetime.datetime.utcnow()
        if not data>=t1 and data<=t2:
            raise forms.ValidationError(
               "Date (From Date) doesn't lie in the interval.")
        return data

    def clean_to_date(self):
        data = self.cleaned_data['to_date']
        # for time-zone aware dates use this one instead:
        #t1 = datetime.datetime(1990,1,1,0,0,0).replace(tzinfo=utc)
        t1 = datetime.datetime(1990,1,1,0,0,0)
        t2 = datetime.datetime.utcnow()
        if not data>=t1 and data<=t2:
            raise forms.ValidationError("Date (To Date) doesn't lie in the interval.")
        return data

    def clean(self):
        cleaned_data = super(ScenarioForm, self).clean()
        t1 = cleaned_data.get("from_date")
        t2 = cleaned_data.get("to_date")
        if t2<t1:
            raise forms.ValidationError(
               "Starting date - %s is bigger then final date - %s." % (t1,t2))
        if 'ncn_id' in self._errors:
            del self._errors['ncn_id']
        return cleaned_data

    def full_clean(self):
        # Disable auto-validation of ncn-id - it is not smart enough to
        #  realise that an existing scenario can keep its own old ncn_id.
        super(ScenarioForm, self).full_clean()
        if 'ncn_id' in self._errors:
            del self._errors['ncn_id']
   
    def __init__(self, *args, **kwargs):
        super(ScenarioForm, self).__init__(*args, **kwargs)
        
        self.fields['scenario_description'].widget = \
            forms.Textarea(attrs={'cols':70,'rows':6})
        self.fields['sensor_type'].widget = forms.TextInput(attrs={'size':18})
        
        self.fields['scenario_name'   ].widget = \
            forms.TextInput(attrs={'size':60})
        self.fields['dsrc'            ].widget = \
            forms.TextInput(attrs={'size':60})
#        self.fields['dsrc_password'   ].widget = forms.PasswordInput()
#        self.fields['preprocessing'   ].widget = forms.CheckboxInput()
#        self.fields['default_script'  ].widget = forms.CheckboxInput()
#        self.fields['cat_registration'].widget = forms.CheckboxInput()
        self.fields['from_date'       ].widget = forms.SplitDateTimeWidget(attrs={'size':11})
        self.fields['to_date'         ].widget = forms.SplitDateTimeWidget(attrs={'size':11})
        self.fields['starting_date'   ].widget = forms.SplitDateTimeWidget(attrs={'size':11})
        self.fields['bb_lc_long'      ].widget = forms.TextInput(attrs={'size':11})
        self.fields['bb_lc_lat'       ].widget = forms.TextInput(attrs={'size':11})
        self.fields['bb_uc_long'      ].widget = forms.TextInput(attrs={'size':11})
        self.fields['bb_uc_lat'       ].widget = forms.TextInput(attrs={'size':11})
        self.fields['cloud_cover'     ].widget = forms.TextInput(attrs={'size':8})
        self.fields['view_angle'      ].widget = forms.TextInput(attrs={'size':8})
        self.fields['repeat_interval' ].widget = forms.TextInput(attrs={'size':8})
        self.fields['default_priority'].widget = forms.TextInput(attrs={'size':8})

        # widget labels
        self.fields['ncn_id'           ].label = 'Unique Id'
        self.fields['scenario_name'    ].label = 'Name'
        self.fields['scenario_description'].label = 'Description'
        self.fields['aoi_type'         ].label = 'AOI'
        self.fields['bb_lc_long'       ].label = 'BBox Lower long'
        self.fields['bb_lc_lat'        ].label = 'BBox Lower lat'
        self.fields['bb_uc_long'       ].label = 'BBox Upper long'
        self.fields['bb_uc_lat'        ].label = 'BBox Upper lat'
        self.fields['from_date'        ].label = 'TOI From'
        self.fields['to_date'          ].label = 'TOI To'
        self.fields['cloud_cover'      ].label = 'Max Cloud Cover'
        self.fields['view_angle'       ].label = 'Max View Angle'
        self.fields['sensor_type'      ].label = 'Sensor Type'
        self.fields['dsrc'             ].label = 'Data Source'
#        self.fields['dsrc_type'        ].label = 'Data Src Type'
#        self.fields['dsrc_login'       ].label = 'Data Src login'
#        self.fields['dsrc_password'    ].label = 'Data Src password'
        self.fields['preprocessing'    ].label = 'S2 atmos. pre-process' 
        self.fields['default_priority' ].label = 'Ingestion priority'
        self.fields['starting_date'    ].label = 'Repeat Starting Date'
        self.fields['repeat_interval'  ].label = 'Repeat Interval(secs)'

        # initial values
        # for time-zone aware dates use this one instead:
        #d2 = datetime.datetime.utcnow().replace(tzinfo=utc)
        d2 = datetime.datetime.utcnow()
        d1 = d2 - datetime.timedelta(days=365)
        self.fields['ncn_id'           ].initial = \
            models.make_ncname(SC_NCN_ID_BASE)
        self.fields['from_date'        ].initial = d1
        self.fields['to_date'          ].initial = d2
        self.fields['starting_date'    ].initial = d2
        self.fields['cloud_cover'      ].initial = 50
        self.fields['view_angle'       ].initial = 50
        self.fields['sensor_type'      ].initial = ""
        self.fields['preprocessing'    ].initial = 1
        self.fields['default_script'   ].initial = 1
        self.fields['default_priority' ].initial = 100
        self.fields['repeat_interval'  ].initial = 0
        self.fields['cat_registration' ].initial = 0
        self.fields['coastline_check'  ].initial = 0


        # not required fields
        self.fields['scenario_description'].required = False
        self.fields['from_date'           ].required = False
        self.fields['starting_date'       ].required = False
        self.fields['to_date'             ].required = False
        self.fields['cloud_cover'         ].required = False
        self.fields['view_angle'          ].required = False
        self.fields['sensor_type'         ].required = False
#        self.fields['dsrc_type'           ].required = False
#        self.fields['dsrc_login'          ].required = False
#        self.fields['dsrc_password'       ].required = False
        self.fields['preprocessing'       ].required = False
        self.fields['default_script'      ].required = False
        self.fields['default_priority'    ].required = False
        self.fields['repeat_interval'     ].required = False
        self.fields['cat_registration'    ].required = False
        self.fields['coastline_check'     ].required = False
