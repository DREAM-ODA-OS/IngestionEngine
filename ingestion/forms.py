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

import models
import django.forms as forms
import datetime
from django.utils.timezone import utc


#class UserForm(forms.ModelForm):
#    class Meta:
#        model = models.Users
#        fields = ['user_name']

#class LoginForm(forms.Form):
#    username = forms.CharField(label='Username',max_length=30)
#    password = forms.CharField(label="Password",widget=forms.PasswordInput())


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


class ScenarioForm(forms.ModelForm):
    class Meta:
        CHOICES = (('1', 'First',), ('2', 'Second',))
        model = models.Scenario
        #fields = '__all__'
        #fields = ['id_scenario','scenario_name','aoi','from_date','to_date','cloud_cover','view_angle','sensor_type','dsrc','dsrc_login','dsrc_password','preprocessing','default_priority']  
        
        exclude = ['id','user']
        #widgets = {
        #    'scenario_description': forms.Textarea(attrs={'cols': 20, 'rows': 10}),
        #    'dsrc_password': forms.PasswordInput(render_value=True),
        #    }

    def clean_cloud_cover(self):
        data = self.cleaned_data['cloud_cover']
        if not data>=0 and data<100:
            raise forms.ValidationError("Max Cloud Cover must be in the interval from 0 to 100.")
        return data

    def clean_view_angle(self):
        data = self.cleaned_data['view_angle']
        if not data>=0 and data<90:
            raise forms.ValidationError("Max View Angle must be in the interval from 0 to 90.")
        return data

    def clean_from_date(self):
        data = self.cleaned_data['from_date']
        t1 = datetime.datetime(1990,1,1,0,0,0).replace(tzinfo=utc)
        t2 = datetime.datetime.utcnow().replace(tzinfo=utc)
        if not data>=t1 and data<=t2:
            raise forms.ValidationError("Date (From Date) doesn't lie in the interval.")
        return data

    def clean_to_date(self):
        data = self.cleaned_data['to_date']
        t1 = datetime.datetime(1990,1,1,0,0,0).replace(tzinfo=utc)
        t2 = datetime.datetime.utcnow().replace(tzinfo=utc)
        if not data>=t1 and data<=t2:
            raise forms.ValidationError("Date (To Date) doesn't lie in the interval.")
        return data

    def clean(self):
        cleaned_data = super(ScenarioForm, self).clean()
        t1 = cleaned_data.get("from_date")
        t2 = cleaned_data.get("to_date")
        if t2<t1:
            raise forms.ValidationError("Starting date - %s is bigger then final date - %s." % (t1,t2))
        return cleaned_data

   
    def __init__(self, *args, **kwargs):
        super(ScenarioForm, self).__init__(*args, **kwargs)
        
        # widgets
        AOI_CHOICES = (
                       ('1', 'Global'),
                       ('2', 'From Map'),
                       ('3', 'From Shapefile'),
                       )
        SENSOR_CHOICES = (
                          ('1','Sentinel 2'),
                          ('2','SPOT 5'),
                          ('3','KOMPAS 2'),
                          ('4','Pleiades'),
                          )
        
        #self.fields['aoi'].widget = forms.CheckboxSelectMultiple(choices=MEDIA_CHOICES)
        self.fields['scenario_description'].widget = forms.Textarea(attrs={'cols':20,'rows':10})
        self.fields['aoi'].widget = forms.RadioSelect(choices=AOI_CHOICES)
        self.fields['sensor_type'].widget = forms.RadioSelect(choices=SENSOR_CHOICES)
        #self.fields['dsrc'].widget = forms.FileInput()
        self.fields['dsrc_password'].widget = forms.PasswordInput()
        #self.fields['preprocessing'].widget = forms.CheckboxInput()
        
        
        # label of widgets
        #self.fields['id'].label = "ID Scenario"
        self.fields['scenario_name'].label = 'Scenario Name'
        self.fields['scenario_description'].label = 'Scenario Description'
        self.fields['aoi'].label = 'AOI'
        self.fields['from_date'].label = 'From'
        self.fields['to_date'].label = 'To'
        self.fields['cloud_cover'].label = 'Max Cloud Cover'
        self.fields['view_angle'].label = 'Max View Angle'
        self.fields['sensor_type'].label = 'Sensor Type'
        self.fields['dsrc'].label = 'Data Source'
        self.fields['dsrc_login'].label = 'Data Src login'
        self.fields['dsrc_password'].label = 'Data Src password'
        self.fields['preprocessing'].label = 'PreProcessing' 
        self.fields['default_priority'].label = 'Default priority'
        self.fields['starting_date'].label = 'Starting Date'
        self.fields['repeat_interval'].label = 'Repeat Interval'

        # initial values
        d2 = datetime.datetime.utcnow().replace(tzinfo=utc)
        d1 = d2 - datetime.timedelta(days=365)
        self.fields['from_date'].initial = d1
        self.fields['to_date'].initial = d2
        self.fields['starting_date'].initial = d2
        self.fields['cloud_cover'].initial = 50
        self.fields['view_angle'].initial = 50
        self.fields['default_priority'].initial = 100
        self.fields['repeat_interval'].initial = 0
        self.fields['preprocessing'].initial = 1

        # not required fields
        #self.fields['id'].required = False

'''
    https://docs.djangoproject.com/en/dev/topics/forms/
    https://docs.djangoproject.com/en/dev/topics/forms/modelforms/
'''
