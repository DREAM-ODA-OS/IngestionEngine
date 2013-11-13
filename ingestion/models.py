############################################################
#  Project: DREAM
#  Module:  Task 5 ODA Ingestion Engine 
#  Author:  Vojtech Stefka  (CVC)
#  Contribution: Milan Novacek (CVC)
#  Creation Date:  Aug 2013
#
#    (c) 2013 Siemens Convergence Creators s.r.o., Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
#  Django models.
#
############################################################

from django.db import models
from django.contrib.auth.models import User

import os

from settings import NCN_ID_LEN, SC_NAME_LEN, SC_DESCRIPTION_LEN, \
    IE_DOWNLOAD_DIR

if not os.access(IE_DOWNLOAD_DIR, os.R_OK|os.W_OK):
    import logging
    logger = logging.getLogger('dream.file_logger')
    logger.info("Cannot write/read "+IE_DOWNLOAD_DIR+", attempting to create.")
    try:
        os.mkdir(IE_DOWNLOAD_DIR,0740)
        logger.info("Created "+IE_DOWNLOAD_DIR)
    except OSError as e:
        logger.error("Failed to create "+IE_DOWNLOAD_DIR+": "+`e`)

#**************************************************
#                  Scenario                       *
#**************************************************
def make_ncname(root):
    n = Scenario.objects.count()
    return root+`n`

AOI_BBOX_CHOICE = 'BB'
AOI_POLY_CHOICE = 'PO'

AOI_CHOICES = (
    (AOI_BBOX_CHOICE, 'Bounding Box'),
    (AOI_POLY_CHOICE, 'Polygon'),
)

class Scenario(models.Model):
    id                   = models.AutoField(primary_key=True)
    ncn_id               = models.CharField(max_length=NCN_ID_LEN)
    scenario_name        = models.CharField(max_length=SC_NAME_LEN)
    scenario_description = models.CharField(max_length=SC_DESCRIPTION_LEN)
    aoi_type             = models.CharField(
        max_length=2,
        choices=AOI_CHOICES,
        default=AOI_BBOX_CHOICE)
    aoi_file             = models.CharField(max_length=1024)
    bb_lc_long           = models.FloatField()
    bb_lc_lat            = models.FloatField()
    bb_uc_long           = models.FloatField()
    bb_uc_lat            = models.FloatField()
    repeat_interval      = models.IntegerField()
    from_date            = models.DateTimeField()
    to_date              = models.DateTimeField()
    starting_date        = models.DateTimeField()
    cloud_cover          = models.FloatField()
    view_angle           = models.FloatField()
    sensor_type          = models.CharField(max_length=96)
    dsrc                 = models.CharField(max_length=1024)
    dsrc_login           = models.CharField(max_length=64)
    dsrc_password        = models.CharField(max_length=64)
    preprocessing        = models.IntegerField()
    default_script       = models.IntegerField()
    default_priority     = models.IntegerField()
    user                 = models.ForeignKey(User)
    
#**************************************************
#                   Script                        *
#**************************************************   
class Script(models.Model): # should be ScenarioScript to make it comprehensible
    id = models.AutoField(primary_key=True)
    script_name = models.CharField(max_length=50)
    script_path = models.CharField(max_length=50) # local address on the django server
    scenario = models.ForeignKey(Scenario)
    


class ProductInfo(models.Model):
    id = models.AutoField(primary_key=True)
    info_status = models.CharField(max_length=50)
    info_error = models.CharField(max_length=200)
    info_date = models.DateTimeField()



#**************************************************
#                 User Script                     *
#**************************************************
def update_script_filename(instance,filename):
    path = "scripts/" # save destination related to .../<ie_project>/media/
    file_name = "%s_%s" % (str(instance.user.id),instance.script_name)
    print "File name: %s" % file_name
    return os.path.join(path,file_name)

class UserScript(models.Model):
    id = models.AutoField(primary_key=True)
    script_name = models.CharField(max_length=50)
    script_file = models.FileField(upload_to=update_script_filename)
    user = models.ForeignKey(User)


#**************************************************
#              Scenario Status                    *
#**************************************************
class ScenarioStatus(models.Model):
    id = models.AutoField(primary_key=True)
    is_available = models.IntegerField() # 1 - available, 0 - not available
    status = models.CharField(max_length=20) # e.g.: downloading, deleting, ...
    done = models.FloatField() # e.g.: 33.3%
    scenario = models.OneToOneField(Scenario)

