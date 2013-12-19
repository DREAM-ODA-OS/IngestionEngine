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

import os

import logging
from django.db import models
from django.contrib.auth.models import User
from django.utils.dateformat import DateFormat

from settings import NCN_ID_LEN, SC_NAME_LEN, SC_DESCRIPTION_LEN, PROD_ERROR_LEN


#**************************************************
#                  Scenario                       *
#**************************************************
def make_ncname(root):
    n = Scenario.objects.count()
    return root+`n`

# ------------ choice types  --------------------------

#  Area Of Interest
AOI_BBOX_CHOICE    = 'BB'
AOI_POLY_CHOICE    = 'PO'
AOI_SHPFILE_CHOICE = 'SH'

AOI_CHOICES = (
    (AOI_BBOX_CHOICE,    'Bounding Box'),
    (AOI_POLY_CHOICE,    'Polygon'),
    (AOI_SHPFILE_CHOICE, 'Shapefile'),
)

# Data Source
DSRC_EOWCS_CHOICE  = 'EO'    # EO-WCS
DSRC_OSCAT_CHOICE  = 'OC'    # OpenSearch Catalogue

DSRC_CHOICES = (
    (DSRC_EOWCS_CHOICE,  'EO-WCS'),
    (DSRC_OSCAT_CHOICE,  'OpenSearch Catalogue'),
)


# ------------ conversion utilities  --------------------------
def date_to_iso8601(src_date):
    return DateFormat(src_date).format("c")

def scenario_dict(db_model):
    """ creates a dictionary from a database model record,
        using selected fields. Converts some fields into
        a more structured representation: e.g. a bbox is
        built up from individual database fields.
    """
    response_data = {}
    for s in (
        "repeat_interval",
        "cloud_cover",
        "view_angle",
        "sensor_type",
        "dsrc",
        "dsrc_type",
        "dsrc_login",
        "dsrc_password",
        "default_priority",
        "default_script",
        "preprocessing",
        "ncn_id"):
        response_data[s] = str(getattr(db_model,s))

    # convert dates to ISO-8601
    response_data["from_date"    ] = date_to_iso8601(db_model.from_date)
    response_data["to_date"      ] = date_to_iso8601(db_model.to_date)
    response_data["starting_date"] = date_to_iso8601(db_model.starting_date)

    if db_model.aoi_type == AOI_BBOX_CHOICE:
        response_data['aoi_bbox'] = {
            'lc' : (db_model.bb_lc_long, db_model.bb_lc_lat),
            'uc' : (db_model.bb_uc_long, db_model.bb_uc_lat)
            }
    else:
        raise UnsupportedBboxError("Unsupported AOI type for scenario id=" +\
                                       db_model.ncn_id)

    return response_data

# ------------  scenario model definition  --------------------------
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
    aoi_poly_lat         = models.CommaSeparatedIntegerField(max_length=1024)
    aoi_poly_long        = models.CommaSeparatedIntegerField(max_length=1024)
    bb_lc_long           = models.FloatField()
    bb_lc_lat            = models.FloatField()
    bb_uc_long           = models.FloatField()
    bb_uc_lat            = models.FloatField()
    from_date            = models.DateTimeField()
    to_date              = models.DateTimeField()
    cloud_cover          = models.FloatField()
    view_angle           = models.FloatField()
    sensor_type          = models.CharField(max_length=96)
    dsrc                 = models.CharField(max_length=1024)
    dsrc_type            = models.CharField(
        max_length=2,
        choices=DSRC_CHOICES,
        default=DSRC_EOWCS_CHOICE)
    dsrc_login           = models.CharField(max_length=64)
    dsrc_password        = models.CharField(max_length=64)
    preprocessing        = models.IntegerField()
    default_script       = models.IntegerField()
    default_priority     = models.IntegerField()
    starting_date        = models.DateTimeField()
    repeat_interval      = models.IntegerField()
    user                 = models.ForeignKey(User)
    
#**************************************************
#                   Script                        *
#**************************************************   
class Script(models.Model): # ScenarioScript 
    id = models.AutoField(primary_key=True)
    script_name = models.CharField(max_length=256)
    script_path = models.CharField(max_length=2048) # local address on the django server
    scenario = models.ForeignKey(Scenario)
    

#**************************************************
#              Product Info                       *
#  Used for the AddProduct operation              *
#**************************************************   
class ProductInfo(models.Model):
    id             = models.AutoField(primary_key=True)
    info_status    = models.CharField(max_length=50)
    info_error     = models.CharField(max_length=PROD_ERROR_LEN)
    info_date      = models.DateTimeField()
    new_product_id = models.CharField(max_length=256)
    product_url    = models.CharField(max_length=4096)



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
    status = models.CharField(max_length=32) # e.g.: downloading, deleting, ...
    done = models.FloatField() # e.g.: 33.3%
    scenario = models.OneToOneField(Scenario)

