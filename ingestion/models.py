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
import random
import datetime

import logging
from django.db import models
from django.contrib.auth.models import User
from django.utils.dateformat import DateFormat

from constants import \
    NCN_ID_LEN, \
    SC_NAME_LEN, \
    SC_DESCRIPTION_LEN, \
    SC_DSRC_LEN, \
    PROD_ERROR_LEN

# Scenario attributes that are exposed for external get operations.
# Also used for some local interfaces.
# More attributes than are in this tuple are exposed, but those
# are processed individually: see scenario_dict() below.
# 
EXT_GET_SCENARIO_KEYS  = (
    "repeat_interval",
    "cloud_cover",
    "view_angle",
    "default_priority",
    "oda_server_ingest",
    "tar_result",
    "cat_registration",
    "download_subset",
    "coastline_check"
)

EXT_GET_SCENARIO_STRINGS = (
    "aoi_type",
    "dsrc_type",
    "dsrc",
    "sensor_type",
    "s2_preprocess",
    "ncn_id"
)
# additional Scenario attributes accepted for /update/new operations,
#  in addition to EXT_GET_SCENARIO_KEYS 
EXT_PUT_SCENARIO_KEYS  = EXT_GET_SCENARIO_KEYS + (
    "scenario_name",
    "scenario_description"
)

#**************************************************
#                  Scenario                       *
#**************************************************
def make_ncname(root):
    n = 0
    try:
        sc_latest = Scenario.objects.latest('id')
        if sc_latest:
            n = sc_latest.id
        else:
            n = 0
    except Scenario.DoesNotExist:
        n = 0

    candidate = root+`n`
    exists = False
    try:
        Scenario.objects.get(ncn_id=candidate)
        exists = True
    except Scenario.DoesNotExist:
        exists = False
    if exists:
        n = 0
        try:
            exid = Scenario.objects. \
                filter(ncn_id__startswith=root). \
                order_by('-ncn_id')[0].ncn_id
            n = int(exid.split(root)[1]) + 1
            candidate = root+`n`
        except Exception:
            n = 0
        if 0==n:
            try:
                # try three times
                candidate = root+`random.randint(10000,99999)`
                Scenario.objects.get(ncn_id=candidate)
                candidate = root+`random.randint(10000,99999)`
                Scenario.objects.get(ncn_id=candidate)
                candidate = root+`random.randint(10000,99999)`
                Scenario.objects.get(ncn_id=candidate)
                logger = logging.getLogger('dream.file_logger')
                logger.warning("Cannot generate a unique ncn_id")
            except Scenario.DoesNotExist:
                pass # all is well - 'does not exist' is exacty what we needed
    return candidate

# ------------ choice types  --------------------------

#  Area Of Interest
AOI_BBOX_CHOICE    = 'BB'
AOI_POLY_CHOICE    = 'PO'
AOI_SHPFILE_CHOICE = 'SH'

AOI_CHOICES = (
    (AOI_BBOX_CHOICE,    'Bounding Box'),
#    (AOI_POLY_CHOICE,    'Polygon'),
#    (AOI_SHPFILE_CHOICE, 'Shapefile'),
)

S2_PRE_NONE = 'NO'
S2_PRE_ATM  = 'AT'

S2_PRE_CHOICES = (
    (S2_PRE_NONE, 'None'),
    (S2_PRE_ATM,  'AtmosCorr'),
)

# Data Source
DSRC_EOWCS_CHOICE  = 'EO'    # EO-WCS
DSRC_BGMAP_CHOICE  = 'BG'    # Background Map
DSRC_OSCAT_CHOICE  = 'OC'    # OpenSearch Catalogue

DSRC_CHOICES = (
    (DSRC_EOWCS_CHOICE,  'EO-WCS'),
    (DSRC_BGMAP_CHOICE,  'Background Map'),
#    (DSRC_OSCAT_CHOICE,  'OpenSearch Catalogue'),
)

TIME_FORMAT_8601   = "%Y-%m-%dT%H:%M:%S"
TIME_FORMAT_f8601   = "%Y-%m-%dT%H:%M:%S.%f"
TIME_FORMAT_Z8601  = "%Y-%m-%dT%H:%M:%S"

# ------------ conversion utilities  --------------------------
def date_to_iso8601(src_date):
    return DateFormat(src_date).format("c")

def date_from_iso8601(src_str):
    l = src_str[-1]
    if not l.isdigit():
        if l=='Z':
            src_str = src_str[:-1]
        else:
            raise ValueError("Bad data format or non-UTC timezone: " + src_str)
    try:
        a = datetime.datetime.strptime(src_str, TIME_FORMAT_f8601)
    except:
        a = datetime.datetime.strptime(src_str, TIME_FORMAT_8601)
    return a

def scenario_dict(db_model):
    """ creates a dictionary from a database model record,
        using selected fields. Converts some fields into
        a more structured representation: e.g. a bbox is
        built up from individual database fields.
    """
    response_data = {}
    for s in ( EXT_GET_SCENARIO_KEYS ):
        response_data[s] = getattr(db_model,s)

    for s in ( EXT_GET_SCENARIO_STRINGS ):
        response_data[s] = getattr(db_model,s).encode('ascii','ignore')

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

    extraconditions = db_model.extraconditions_set.all()
    extras_list = []
    for e in extraconditions:
        extras_list.append( ( e.xpath.encode('ascii','ignore'),
                              e.text.encode('ascii','ignore')) )
    response_data['extraconditions'] = extras_list

    dssids = []
    dss_set = db_model.eoid_set.filter(selected=True)
    for dssid in dss_set:
        dssids.append(dssid.eoid_val.encode('ascii','ignore'))
    response_data['dssids'] = dssids

    return response_data

# ------------  scenario model definition  --------------------------
class Scenario(models.Model):
    #
    #  Caution:  the editScenario form relies on the order of the
    #   fields - rearranging the order will break the rendering of
    #   the table in the django-based standalone admin client
    #
    id                   = models.AutoField(primary_key=True)
    ncn_id               = models.CharField(max_length=NCN_ID_LEN, unique=True)
    scenario_name        = models.CharField(max_length=SC_NAME_LEN)
    scenario_description = models.CharField(max_length=SC_DESCRIPTION_LEN)
    dsrc                 = models.CharField(max_length=SC_DSRC_LEN)
    dsrc_type            = models.CharField(
        max_length=2,
        choices=DSRC_CHOICES,
        default=DSRC_EOWCS_CHOICE)
    dsrc_login           = models.CharField(max_length=64)
    dsrc_password        = models.CharField(max_length=64)
    aoi_type             = models.CharField(
        max_length=2,
        choices=AOI_CHOICES,
        default=AOI_BBOX_CHOICE)
    coastline_check      = models.BooleanField()
#    aoi_file             = models.CharField(max_length=1024)
#    aoi_poly_lat         = models.CommaSeparatedIntegerField(max_length=1024)
#    aoi_poly_long        = models.CommaSeparatedIntegerField(max_length=1024)
    bb_lc_long           = models.FloatField()
    bb_lc_lat            = models.FloatField()
    bb_uc_long           = models.FloatField()
    bb_uc_lat            = models.FloatField()
    from_date            = models.DateTimeField()
    to_date              = models.DateTimeField()
    cloud_cover          = models.FloatField()
    view_angle           = models.FloatField()
    sensor_type          = models.CharField(max_length=96)
    oda_server_ingest    = models.BooleanField()
    tar_result           = models.BooleanField()
    cat_registration     = models.BooleanField()
    s2_preprocess        = models.CharField(
        max_length=2,
        choices=S2_PRE_CHOICES,
        default=S2_PRE_NONE)
    download_subset      = models.BooleanField()
    default_priority     = models.IntegerField()
    repeat_interval      = models.IntegerField()
    starting_date        = models.DateTimeField()
    user                 = models.ForeignKey(User)

#*****************************************************
#                   Archive                          *
#  Archive of products already downloaded            *
#  used to avoid re-downloading what we already have.*
#  Stores just the EOID.                             *
#*****************************************************
class Archive(models.Model):
    id           = models.AutoField(primary_key=True)
    scenario     = models.ForeignKey(Scenario)
    eoid         = models.CharField(max_length=2048)


#**************************************************
#                   Eoid                          *
#  List of EOIDS for a scenario selected by       *
#  the user, used to restrict product selection   *
#**************************************************   
class Eoid(models.Model):
    id       = models.AutoField(primary_key=True)
    scenario = models.ForeignKey(Scenario)
    eoid_val = models.CharField(max_length=2048)
    selected = models.BooleanField()


#**************************************************
#                   ExtraConditions               *
#  List of Extra conditions used to select        *
#  product urls from the metadata                 *
#**************************************************   
class ExtraConditions(models.Model):
    id        = models.AutoField(primary_key=True)
    scenario  = models.ForeignKey(Scenario)
    xpath     = models.CharField(max_length=3072)
    text      = models.CharField(max_length=2500)


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
    logger = logging.getLogger('dream.file_logger')
    path = "scripts/" # save destination related to .../<ie_project>/media/
    file_name = "%s_%s" % (str(instance.user.id),instance.script_name)
    logger.debug ("File name: %s" % file_name)
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
    status = models.CharField(max_length=32) # e.g.: IDLE, deleting, ...
    done = models.FloatField() # e.g.: 33.3%
    scenario = models.OneToOneField(Scenario)
    active_dar = models.CharField(max_length=256)
    ingestion_pid = models.IntegerField()

