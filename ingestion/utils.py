###########################################################
#  Project: DREAM
#  Module:  Task 5 ODA Ingestion Engine 
#  Author:  Milan Novacek  (CVC)
#  Date:    Aug 20, 2013
#
#    (c) 2013 Siemens Convergence Creators s.r.o., Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
#  Miscellenaous utilities for ingestion logic.
#
###########################################################

import time, calendar, random
from django.utils.dateformat import DateFormat
import models

# ------------ Exceptions  --------------------------
class NoEPSGCodeError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class IngestionError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class UnsupportedBboxError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

# ------------ Bbox --------------------------
class Bbox:
    # A fully defined instance should have members ll and ur
    #  ll and ur are tuples  (x,y).
    def __init__(self, ll, ur):
        self.ll = ll
        self.ur = ur

    def __str__(self):
        return `self.ll` +","+ `self.ur`

    def __repr__(self):
        return `self.ll` +","+ `self.ur`

    def overlaps(self, bb2):
        if (self.ll[1] > bb2.ur[1]) or (self.ll[0] > bb2.ur[0]): return False
        if (self.ur[1] < bb2.ll[1]) or (self.ur[0] < bb2.ll[0]): return False
        return True

#factory
def bbox_from_strings(lc, uc, order_xy = True):
    if order_xy:
        i = 0
        j = 1
    else:
        i = 1
        j = 0

    bbox = None
    try:
        llc = lc.lstrip().split(' ')
        urc = uc.lstrip().split(' ')
        bbox = Bbox(
            (float(llc[i]), float(llc[j])),
            (float(urc[i]), float(urc[j])) )
    except:
        log("error parsing bbox corners")
        return None
    return bbox
    
# ------------ time periods   --------------------------
class TimePeriod:
    # only UTC times are supported
    TIME_FORMAT_8601   = "%Y-%m-%dT%H:%M:%S"
    TIME_FORMAT_SIMPLE = "%Y-%m-%d %H:%M:%S"

    def __init__(self, begin, end=None):

        BTF = None
        ETF = None

        if begin[-1]  == 'Z'     : begin = begin[:-1]
        if begin[-6:] == "+00:00": begin = begin[:-6]
        if begin[10]  == ' ':
            BTF = self.TIME_FORMAT_SIMPLE
        else:
            BTF = self.TIME_FORMAT_8601

        self.begin_str  = begin
        self.begin_time = calendar.timegm(
            time.strptime(begin, BTF))

        if None==end:
            self.end_str  = begin
            self.end_time = self.begin_time
        else:
            if end[-1]  == 'Z'     : end = end[:-1]
            if end[-6:] == "+00:00": end = end[:-6]
            if end[10]  == ' ':
                ETF = self.TIME_FORMAT_SIMPLE
            else:
                ETF = self.TIME_FORMAT_8601
            self.end_str  = end
            self.end_time = calendar.timegm(
                time.strptime(end, ETF))

        if self.begin_time > self.end_time:
            tmp = self.begin_time
            self.begin_time = self.end_time
            self.end_time = tmp


    def __str__(self):
        return self.begin_str +", "+self.end_str

    def __repr__(self):
        return self.begin_str +", "+self.end_str


    def overlaps(self, t2):
        if None == t2: return True
        if self.begin_time > t2.end_time: return False
        if self.end_time   < t2.begin_time: return False
        return True

# ------------ data handling   --------------------------
def date_to_iso8601(src_date):
    return DateFormat(src_date).format("c")

def scenario_dict(db_model):
    """ creates a dictionary from a database model record """
    response_data = {}
    for s in (
        "repeat_interval",
        "cloud_cover",
        "view_angle",
        "sensor_type",
        "dsrc",
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

    if db_model.aoi_type == models.AOI_BBOX_CHOICE:
        response_data['aoi_bbox'] = {
            'lc' : (db_model.bb_lc_long, db_model.bb_lc_lat),
            'uc' : (db_model.bb_uc_long, db_model.bb_uc_lat)
            }
    else:
        raise UnsupportedBboxError("Unsupported BBOX type for scenario id=" +\
                                       db_model.ncn_id)

    return response_data

# ------------ tmp name generating function  --------------------------
def mkFname(base):
    st_time = time.localtime()
    fn = base + `(st_time.tm_year - 2010)` + \
        '%03d'%st_time.tm_yday + '_' + \
        '%02d'%st_time.tm_hour + \
        '%02d'%st_time.tm_min + \
        '%02d'%st_time.tm_sec + \
        chr(random.randrange(ord('a'), ord('z'))) + \
        chr(random.randrange(ord('a'), ord('z'))) + \
        chr(random.randrange(ord('a'), ord('z'))) + \
        chr(random.randrange(ord('a'), ord('z'))) + \
        chr(random.randrange(ord('a'), ord('z'))) + \
        chr(random.randrange(ord('a'), ord('z')))
    return fn
