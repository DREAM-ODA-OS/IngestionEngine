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
import os, shutil
import urllib2
from django.utils.dateformat import DateFormat
import models

# ------------ Exceptions  --------------------------
class NoEPSGCodeError(Exception):
    pass

class IngestionError(Exception):
    pass

class DMError(Exception):
    pass

class UnsupportedBboxError(Exception):
    pass

# ------------ Process Id Utilities  ------------------
def pid_is_valid(pid):
    if 0 == pid: return False
    return `pid` in os.listdir('/proc')

def find_process_ids(match_strings):
    """ cmdline of the returned pid matches all match_strings """
    ret_pids = []
    pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]
    for pid in pids:
        try:
            fp = open(os.path.join('/proc', pid, 'cmdline'), 'rb')
            cmdline = fp.read()
            fp.close()
        except:
            continue
        # must match all match_strings
        match = True
        for s in match_strings:
            if not s in cmdline:
                match = False
                break
        if match: ret_pids.append(pid)
    return ret_pids


# ------------ Download Manager Properties Handling  ------------------
def read_props(fn):
    """ returns the lines in the file as list, and the
    value of the BASE_DOWNLOAD_FOLDER_ABSOLUTE property,
    including its index in the lines list """
    f = open(fn, "r")
    lines = []
    download_folder_prop = None
    download_folder_line = -1
    dm_listen_port_prop  = None
    dm_listen_port_line  = -1
    i = 0
    for l in f:
        lines.append(l)
        if l[0] != "#":
            kv = l.split('=')
            if kv[0] == "BASE_DOWNLOAD_FOLDER_ABSOLUTE":
                download_folder_prop = kv[1].strip()
                download_folder_line = i
            if kv[0] == "WEB_INTERFACE_PORT_NO":
                dm_listen_port_prop = kv[1].strip()
                dm_listen_port_line = i
        i += 1
    f.close()
    return (lines, download_folder_prop, download_folder_line, 
            dm_listen_port_prop, dm_listen_port_line)

def backup_props(dm_config_file, logger):
    """ if the .BAK file already exists then don't back up again """
    backup_fn = dm_config_file + ".BAK"
    if not os.access(backup_fn, os.F_OK):
        shutil.move(dm_config_file, backup_fn)
        logger.info("Backed up DM's config file to "+backup_fn)
    else:
        logger.warning(
            "Not backing up DM's config, a backup file already exists (" +
            backup_fn+")")

def write_props(dm_config_file, lines, logger):
    f = open(dm_config_file, "w")
    for l in lines:
        f.write(l)
    f.close()
    logger.info("Wrote a new DM config file to "+dm_config_file)
    
def setup_dm_config(
    dm_config_file,
    download_dir,
    dm_port,
    logger):
    """ checks and possilby sets the download manager's idea
        of where to dowload products to.
        Returns the DM's listening port """
    lines, dm_dir, dmd_line, old_dm_port, dm_port_line = read_props(dm_config_file)
    if len(lines) < 1:
        raise Exception("Zero length dm properties, fn="+dm_config_file)
    if dm_dir == None or dmd_line < 0:
        raise Exception(
            "BASE_DOWNLOAD_FOLDER_ABSOLUTE not found in properties, fn="+dm_config_file)

    props_changed = False

    if dm_dir != download_dir:
        logger.info("Setting Download Manager's " +
                    "BASE_DOWNLOAD_FOLDER_ABSOLUTE" +
                    " to "+download_dir + "\n" +
                    "    Old setting was:\n" + lines[dmd_line]
                    )
        lines[dmd_line] = "BASE_DOWNLOAD_FOLDER_ABSOLUTE="+download_dir+"\n"
        props_changed = True
    
    if int(old_dm_port) != dm_port:
        logger.info("Setting Download Manager's Port to " + `dm_port`)
        lines[dm_port_line] = "WEB_INTERFACE_PORT_NO="+`dm_port`
        props_changed = True

    if props_changed:
        backup_props(dm_config_file, logger)
        write_props(dm_config_file, lines, logger)

    return `dm_port`

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

# ------------ internet access --------------------------
def read_from_url(
    url,
    post_data=None,
    max_size=851200,    #bytes, 0 for unlimited
    read_timeout=300    #seconds, 0 for unlimited
    ):
    resp = None
    r = urllib2.urlopen(url, post_data)
    end_time = time.time() + read_timeout
    blk_sz = 8192
    while True:
        buffer = r.read(blk_sz)
        if not buffer:
            break
        if read_timeout > 0 and time.time() > end_time:
            raise IngestionError("URL read time expired")
        if None == resp: resp = buffer
        else:            resp += buffer
        if max_size > 0 and None != resp and len(resp) > max_size:
            raise IngestionError("Max read size exceeded")
    if None != r: r.close()
    return resp

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
