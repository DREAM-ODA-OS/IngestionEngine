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
#  Miscellaneous utilities for ingestion logic.
#
###########################################################

import sys
import os
import os.path
from os import open   as osopen
from os import fdopen
from os import O_WRONLY, O_CREAT, O_EXCL
import shutil
import glob
import re
import urllib2
import traceback
import time, calendar
import random
from math import floor
import subprocess
from osgeo import osr

BLK_SZ = 8192
MAX_MANIF_FILES = 750000
MANIFEST_FN = "MANIFEST"
META_SUFFIX = ".meta"
DATA_SUFFIX = ".data"
TIFF_SUFFIX = '.tiff'
TIF_SUFFIX  = '.tif'

# ------------ osr Init  ------------
SPATIAL_REF_WGS84 = osr.SpatialReference()
SPATIAL_REF_WGS84.SetWellKnownGeogCS( "EPSG:4326" )


# ------------ Exceptions  --------------------------
class NoEPSGCodeError(Exception):
    pass

class IngestionError(Exception):
    pass

class ManageScenarioError(Exception):
    pass

class StopRequest(Exception):
    pass

class DMError(Exception):
    pass

class ConfigError(Exception):
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


# ------------ tmp name generating functions  --------------------------
B60_SYMBOLS = "0123456789abcdefghijkmnopqrstuvwxyzABCDEFGHIJKLMNPQRSTUVWXYZ"
LEN_LET = len(B60_SYMBOLS)

def letter_encode(i):
    return B60_SYMBOLS[i]

def mkFname(base):
    tnow = time.time()
    ms = "%03d" % int(1000*(tnow-floor(tnow)))
    st_time = time.gmtime(tnow)
    fn = base + \
        '%02d'%st_time.tm_mday + '_' + \
        '%02d'%st_time.tm_hour + \
        '%02d'%st_time.tm_min + \
        '%02d'%st_time.tm_sec + '_' + ms +\
        chr(random.randrange(ord('a'), ord('z'))) + \
        chr(random.randrange(ord('a'), ord('z'))) + \
        chr(random.randrange(ord('a'), ord('z'))) + \
        chr(random.randrange(ord('a'), ord('z'))) + \
        chr(random.randrange(ord('a'), ord('z')))
    return fn


def mkIdBase():
    st_time = time.gmtime()
    return `(st_time.tm_year - 2010)` + \
        '%03d'%st_time.tm_yday + \
        '%02d'%st_time.tm_hour + \
        '%02d'%st_time.tm_min + \
        '%03d'% + random.randrange(1000) + "_"

def mk_unique_fn(dir_path, leaf_root, maxtries):
    full_path = os.path.join(dir_path, leaf_root)
    i = 0
    while os.path.exists(full_path):
        i += 1
        mm = leaf_root + "_" + `i`
        full_path = os.path.join(dir_path, mm)
        if i > maxtries:
            raise IngestionError(
                "Too many non_unique files (>"+`maxtries`+"), " +
                "last_tried: '"+full_path+"'")
    return full_path

def open_unique_file(dir_path, leaf_root, maxtries):
    i = 0
    fname = mk_unique_fn(dir_path, leaf_root, maxtries)
    err_str = None
    while i<64:
        i += 1
        try:
            fd = osopen(fname, O_WRONLY | O_CREAT | O_EXCL, 0640)
            fp = fdopen(fd,"w")
            return fp, fname
        except OSError as e:
            err_str = `e`
            print err_str
        try:
            fname = mk_unique_fn(dir_path, leaf_root+"_"+`i`+"_", 8)
        except IngestionError:
            pass
    # exhausted all attempts
    raise IngestionError("Cannot open unique file: "+ fname+\
                             ", error: "+err_str)


# ------------ Download Manager Properties Handling  ------------------
def read_props(fn):
    """ returns the lines in the file as list, and the
    values of BASE_DOWNLOAD_FOLDER_ABSOLUTE and WEB_INTERFACE_PORT_NO.
    including their idices in the lines list """
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

def get_dm_config(
    dm_config_file,
    logger):
    """ get the download manager's listening port and download dir.
    """
    lines, dm_dir, dmd_line, dm_port, dm_port_line = read_props(dm_config_file)
    if len(lines) < 1:
        raise Exception("Zero length dm properties, fn="+dm_config_file)
    if dm_dir == None or dmd_line < 0:
        raise Exception(
            "BASE_DOWNLOAD_FOLDER_ABSOLUTE not found in properties, fn="+dm_config_file)

    return (dm_port, dm_dir)

# ------------ Coordinates -------------------
def coords_from_text(coord_string, srs=None):
    """
     input is a text containing x y pairs with just whitespace separating
     all values, e.g.:
       42.802 -1.8192  42.804 -1.762
     epsg is the coorsystem to convert to, this is done on the fly
     to improve efficiency.  No conversion is done if srsNumber is None.
     returns a list containing the (x, y) pairs
     No error checking is done; simply throws an exception in case
     of problems.
    """
    ret = []
    if not coord_string: return ret

    if srs and not srs_is_WGS84(srs):
        srcRef = get_spatialReference(srs)
        ct = osr.CoordinateTransformation(srcRef, SPATIAL_REF_WGS84)
    else:
        ct = None

    vals = coord_string.split()
    n = len(vals)
    i = 0
    while i < n:
        x = float(vals[i])
        y = float(vals[i+1])

        if ct:
            xy = ct.TransformPoint(x, y)
        else:
            xy = (x, y)

        ret.append(xy)
        i += 2

    return ret


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

    def equals(self, other):
        return \
            self.ll[0] == other.ll[0] and \
            self.ll[1] == other.ll[1] and \
            self.ur[0] == other.ur[0] and \
            self.ur[1] == other.ur[1]

    def equals_corners(self, ll, ur):
        return \
            self.ll[0] == ll[0] and \
            self.ll[1] == ll[1] and \
            self.ur[0] == ur[0] and \
            self.ur[1] == ur[1]

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
    # fields:
    #   begin_time, begin_str
    #   end_time,   end_str
    #
    # only UTC times are supported
    TIME_FORMAT_8601   = "%Y-%m-%dT%H:%M:%S"
    TIME_FORMAT_SIMPLE = "%Y-%m-%d %H:%M:%S"

    def to_timegm(self, timestr):

        TF = None

        if timestr[-1]  == 'Z'     : timestr = timestr[:-1]
        if timestr[-6:] == "+00:00": timestr = timestr[:-6]

        # Look for a possible fraction on seconds
        # if we find one, then drop the fractions.
        time_dot_i = timestr.rfind('.')
        if time_dot_i < 0:
            time_dot_i = timestr.rfind(',')

        if time_dot_i > 0:
            timestr = timestr[:time_dot_i-len(timestr)]

        if timestr[10]  == ' ':
            TF = self.TIME_FORMAT_SIMPLE
        else:
            TF = self.TIME_FORMAT_8601

        return calendar.timegm( time.strptime(timestr, TF) )
        

    def __init__(self, begin, end=None):

        self.begin_str  = begin
        self.begin_time = self.to_timegm(begin)

        if None==end:
            self.end_str  = begin
            self.end_time = self.begin_time
        else:
            self.end_str  = end
            self.end_time = self.to_timegm(end)

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

    def equals_se(self, start, end):
        return self.begin_time == start and self.end_time == end

def build_aoi_toi(aoi_json, start_tp, end_tp):
    bb_ll = ( float(aoi_json["lc"][0]), float(aoi_json["lc"][1]) )
    bb_ur = ( float(aoi_json["uc"][0]), float(aoi_json["uc"][1]) )
    ret_aoi = Bbox( bb_ll, bb_ur )
    ret_time = TimePeriod(start_tp, end_tp)
    return ret_aoi, ret_time
    
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
    while True:
        buff = r.read(BLK_SZ)
        if not buff:
            break
        if read_timeout > 0 and time.time() > end_time:
            raise IngestionError("URL read time expired")
        if None == resp: resp = buff
        else:            resp += buff
        if max_size > 0 and None != resp and len(resp) > max_size:
            raise IngestionError("Max read size exceeded")
    if None != r: r.close()
    return resp

# ------------ file utils: splitting, data handling  -----------------
def read_headers(fp):
    headers = {}
    nl = re.compile('\r?\n')

    l = fp.readline()
    i = 0
    while not nl.match(l):
        i += 1
        kv = l.split(":", 1)
        if len(kv) > 1:
            headers[kv[0]] = kv[1].strip()
        if i>256:
            raise IngestionError("More than 256 headers encountered")
        l = fp.readline()

    return headers


def split_wcs_raw(path, f, logger):
    """ TEMPORARY: splits a file into mime/multipart parts.
        path is the directory where the product is dowloaded to and
             also where the new metadata, and data files are to be
             created.
        f  is the mime/multipart source as downloaded by the DM.
        The algorithm more or less expects the boundary to be '--wcs',
        but will check and try with whatever it finds in the
        first line.
        The file must start with a line that is the boundary,
        followed by a line starting with "Content-Type:"
        follwed by a blank line,
        follwed by a line starting with '<?xml'.
    """
    manif_str = None
    fn = os.path.join(path,f)
    fp = open(fn, "r")
    meta_fp = None
    data_fp = None
    meta_fname = None
    data_fname = None
    delete_orig = True

    # These headers from the data part will be put in the manifest file
    disclose_headers = [
        "Content-Type",
        "Content-Description",
        "Content-Disposition",
        "Content-Id",
        ]

    try:
        l1 = fp.readline()

        top_hdrs = read_headers(fp)
        
        if len(top_hdrs) < 1:
            raise IngestionError("No HTTP/mime headers found - Unexpected file contents in: "+fn)
        if not "Content-Type" in top_hdrs:
            raise IngestionError("No Content-Type header found in "+fn)

        boundary = None
        m = re.match('--wcs\r?\n', l1)
        if None != m:
            boundary = m.group()
        else:
            m = re.match('\S\r?\n', l1)
            if None != m: boundary = m.group()
        if None == boundary:
            raise IngestionError("Initial boundary not found, f=: "+fn)

        # create meta-data file
        meta_type = top_hdrs['Content-Type'].strip()
        meta_fname = fn + META_SUFFIX
        if os.path.exists(meta_fname):
            # something is wrong if this name already exists,
            # just bail rather than try to create a unique one.
            raise IngestionError("File exists: "+meta_fname)

        meta_fp = open(meta_fname,"w")
        l = fp.readline()
        if not l.startswith('<?xml'):
            logger.warning("No xml start tag found in "+fn)
        while l != boundary:
            meta_fp.write(l)
            l = fp.readline()
        meta_fp.close()

        eol = None
        if boundary[-2:-1] == '\r': eol = '\r\n'
        else:                       eol = '\n'
        raw_bound = eol+boundary.rstrip()

        # create data file
        hdrs = read_headers(fp)
        data_fname = None
        if len(hdrs) < 1:
            logger.warning("No HTTP/mime headers found after boundary ('"+
                           boundary.rstrip()+"') in file " + f)
            delete_orig = False
        elif "Content-Disposition" in hdrs \
                and "filename=" in hdrs["Content-Disposition"]:
            cd = hdrs["Content-Disposition"].split(";")
            data_fname = None
            fnstring = None
            for s in cd:
                if "filename=" in s:
                    fnstring = s.split("filename=")[1].strip()
                    break
            if not fnstring:
                raise IngestionError(
                    "'filename=' not found in 'Content-Disposition' header"+ \
                        "headers: "+`hdrs`)
            if fnstring.startswith('/') or '../' in fnstring:
                raise IngestionError("Bad filename='"+fnstring+"'")
            if len(cd) == 2:
                data_fname = os.path.join(path, fnstring)

        if None == data_fname:
            data_fname = fn+DATA_SUFFIX
            delete_orig = False

        if os.path.exists(data_fname):
            # something is wrong if this name already exists,
            # just bail rather than try to create a unique one.
            raise IngestionError("File exists: "+data_fname)

        data_fp = open(data_fname, "w")
        buff = None
        while True:
            buff = fp.read(BLK_SZ)
            if not buff:
                break
            if raw_bound in buff:
                break
            data_fp.write(buff)
        if not buff:
            logger.warning("Unexpected EOF while splitting "+fn)
            delete_orig = False
        else:
            rests = buff.split(raw_bound)
            if len(rests) < 2:
                logger.warning("Failed to separate colosing boundary, f=" + fn)
                delete_orig = False
            elif len(rests[1]) > 4:
                logger.warning("unexpected trailing chars after boundary, f="+fn)
                delete_orig = False
            data_fp.write(rests[0])

        data_fp.close()

        if delete_orig:
            os.unlink(fn)

        extra_manifest = ''
        for h in disclose_headers:
            if h in hdrs:
                extra_manifest += h + '="' + hdrs[h] + '"\n'

        manif_str = \
            'METADATA="'+meta_fname + '"\n' + \
            'META_TYPE="'+meta_type + '"\n' + \
            'DATA="'+data_fname     + '"\n' + \
            extra_manifest
        
        return manif_str, meta_fname, data_fname

    except Exception as e:
        fp.close()
        if meta_fp != None: meta_fp.close()
        if data_fp != None: data_fp.close()
        logger.error("Exception while splitting file '"+f+"': "+`e`)
        logger.debug(traceback.format_exc(4))
        return None, None

def write_manifest_file(dir_path, manif_str):
    mf_name = os.path.join(dir_path, MANIFEST_FN)
    if os.path.exists(mf_name):
        logger.warning("MANIFEST file already exists in "+dir_path+", " +
                   "Creating another one")
        i = 0
        while os.path.exists(mf_name):
            i += 1
            mm = "MANIFEST_" + `i`
            mf_name = os.path.join(dir_path, mm)
            if i > MAX_MANIF_FILES:
                raise IngestionError(
                    "Too many manifest files (>"+`MAX_MANIF_FILES`+")")

    mf_fp = open(mf_name,"w")
    mf_fp.write(manif_str)
    mf_fp.close()
    return mf_name

def create_manifest(logger,
                    ncn_id,
                    dir_path,
                    metadata=None,
                    data=None,
                    orig_data=None):
    # the manifest should contain lines according to the following example:
    #    SCENARIO_NCN_ID="ncn_id"
    #    DOWNLOAD_DIR="/path/p_scid0_001"
    #    METADATA="/path/p_scid0_001/ows.meta"
    #    DATA="/path/p_scid0_001/p1.tif"
    #

    manif_str = \
        'SCENARIO_NCN_ID="'+ ncn_id        + '"\n' + \
        'DOWNLOAD_DIR="'   + dir_path      + '"\n'
    if metadata:
        full_metadata = os.path.join(dir_path, metadata)
        manif_str += 'METADATA="'       + full_metadata + '"\n'
    if data:
        full_data = os.path.join(dir_path, data)
        manif_str += 'DATA="'           + full_data     + '"\n'
    if orig_data:
        full_orig = os.path.join(dir_path, orig_data)
        manif_str += 'ORIG_DATA="'      + full_orig     + '"\n'

    mf_name = write_manifest_file(dir_path, manif_str)
    return mf_name

# ------------ product handling  --------------------------
# Split each downloaded product into its parts and generate
#  a product manifest for the ODA server
# TODO: the splitting should be done by the EO-WCS DM plugin
#       instead of doing it here
#
def split_and_create_mf(dir_path, ncn_id, logger):

    manif_str = ''
    metafiles = []
    files = os.listdir(dir_path)
    if len(files) > 1:
        logger.warning("Found " + `len(files)` + " in " + dir_path + \
                           ", expect 1.")
    for f in files:
        if f.startswith(MANIFEST_FN) or f.endswith(META_SUFFIX) or f.endswith(DATA_SUFFIX):
            logger.warning("Ingestion: ignoring "+f)
            continue
        if f.endswith(TIFF_SUFFIX) or f.endswith(TIF_SUFFIX):
            logger.warning("Ingestion: Unexpected TIFF file, ignoring: "+f)
            continue

        ret_str, metafile, df = split_wcs_raw(dir_path, f, logger)
        if not ret_str:
            logger.error("Failed to split file '"+f+"'.")
            continue
        else:
            manif_str += ret_str
            metafiles.append(metafile)

    if manif_str:
        manif_str = \
            'SCENARIO_NCN_ID="'+ncn_id + '"\n' + \
            'DOWNLOAD_DIR="'+ dir_path + '"\n' + \
            manif_str
        mf_name = write_manifest_file(dir_path, manif_str)
        return mf_name, metafiles

    else:
        return None, None


# ------------ Directory check access & create  --------------------------
def make_new_dir(dir_path, logger):
    try:
        os.mkdir(dir_path,0740)
        logger.info("Created "+dir_path)
    except OSError as e:
        msg = "Failed to create "+dir_path+": "+`e`
        logger.error(msg)
        raise  IngestionError(msg)

def check_or_make_dir(dir_path, logger):
    if not os.access(dir_path, os.R_OK|os.W_OK):
        if logger:
            logger.debug("Cannot write/read "+dir_path+", attempting to create.")
        try:
            os.mkdir(dir_path,0740)
            if logger:
                logger.info("Created "+dir_path)
        except OSError as e:
            msg = "Failed to create "+dir_path+": "+`e`
            if logger:
                logger.error(msg)
            raise  IngestionError(msg)

def get_base_fname(orig_full_path):
    return  os.path.basename(orig_full_path)

# ------------ Unix system Proc access  --------------------------
def check_listening_port(port):
    SYSTEM_PROC_NET_PATH   = "/proc/net/tcp"
    PROC_UID_INDEX         = 7
    PROC_STATUS_INDEX      = 3
    PROC_ADDRESS_INDEX     = 1

    found = False
    try:
        uid = "%d" % os.getuid()
        proc = open(SYSTEM_PROC_NET_PATH, "r")
        for l in proc:
            fields = l.split()
            if fields[PROC_UID_INDEX] == uid and \
                    fields[PROC_STATUS_INDEX] == '0A':
                p = int(fields[PROC_ADDRESS_INDEX].split(":")[1],16)
                if p == port:
                    found = True
                    break
        proc.close()
        proc = None
    except Exception as e:
        pass
    finally:
        if None != proc: proc.close()
    return found
   
# ------------ spatial references cache --------------------------

spatial_refs = {}

def get_spatialReference(srsName):
    ref = None
    try:
        ref = spatial_refs[srsName]
    except KeyError:
        if srsName.startswith("http://www.opengis.net/def/crs/EPSG"):
            epsg = (srsName.split('/')[-1])
            srsName = 'EPSG:'+epsg
        ref = osr.SpatialReference()
        ret = ref.SetFromUserInput(srsName)
        if 0 != ret:
            raise NoEPSGCodeError("Unknown EPSG code "+srsName)
        spatial_refs[srsName] = ref
    return ref

def srs_is_WGS84(srs):
    try:
        ref = get_spatialReference(srs)
        return ref.IsSame(SPATIAL_REF_WGS84)

    except Exception as e:
        return False
   

def bbox_to_WGS84(srs, bbox):
    if srs_is_WGS84(srs): return
    srcRef = get_spatialReference(srs)
    ct=osr.CoordinateTransformation(srcRef, SPATIAL_REF_WGS84)
    new_ll = ct.TransformPoint(bbox.ll[0], bbox.ll[1])
    new_ur = ct.TransformPoint(bbox.ur[0], bbox.ur[1])
    bbox.ll = (new_ll[0], new_ll[1])
    bbox.ur = (new_ur[0], new_ur[1])

# ------------ untar/unzip  --------------------------
IE_TARZIP_COMMANDS = {
    '.tgz'    :  ['tar', 'xzf'],
    '.gz'     :  ['gunzip'],
    '.tar'    :  ['tar', 'xf' ],
    '.zip'    :  ['unzip'],
#    '.tbz2'   :  
#    '.tbz'    :  
    }

def ie_unpack_maybe(targetdir, data):
    # if data is a tar, zip or gzip file with a known extension,
    #  ('.tgz' , '.gz' , '.tar', '.zip')
    # then we atempt to untar, ungzip or unzip data the data.
    # The resulting unpacked directory or file is assumed to be
    #  same as the basename of the input file (e.g. 'foo.tgz'
    #  unpacks to 'foo'.
    #  The basename of the tar archive is ultimately passed to the
    #  ODA registration script as the 'data' name.
    #  If the file has no extension, then no action is performed, except
    #  to check it the file exists.
    #  Returns: The unpacked filename is returned, or the original
    #  name if no unpacking was done.
    #  If the input data file does not exist, return 'None'.
    #

    fullpath = os.path.join(targetdir, data)
    if not os.path.exists(fullpath):
        return None

    fn, ext = os.path.splitext(data)

    # Don't unpack unless we have a known extension
    if not ext or not ext in IE_TARZIP_COMMANDS:
        return data

    # handle the case of '.tar.gz'
    if '.gz' == ext:
        fn2, ext2 = os.path.splitext(fn)
        if '.tar' == ext2:
            ext = '.tgz'
            fn = fn2

    cmd = []
    for c in IE_TARZIP_COMMANDS[ext]:
        cmd.append(c)
    cmd.append(data)

    curr_dir = os.getcwd()
    os.chdir(targetdir)
    r = subprocess.call(cmd)
    if 0 == r:
        os.unlink(data)
    os.chdir(curr_dir)
    if 0 != r:
        print >>sys.stderr, "Non-zero cmd status ie_unpack_maybe: "  + `r`
        raise IngestionError ("Unpacking failed, status: " + `r`)

    # The basenae of the tar archive is passed to ODA as
    # the entry 'DATA= ' in the MANIFEST file.
    return fn

def get_glob_list(targetdir, suffix):
    curr_dir = os.getcwd()
    os.chdir(targetdir)
    glob_list = glob.glob("*"+suffix)
    os.chdir(curr_dir)
    return glob_list

def extract_outfile(src_str):
    outfile_str = ''
    if src_str.startswith("-outfile="):
        outfile_str = src_str.split("-outfile=")[1]
    else:
        outfile_str = src_str
    return outfile_str

# ------------ Dummy logger  --------------------------

class DummyLogger():
    def info(self,s):   print "INFO: "+s
    def warning(self,s):print "WARN: "+s
    def error(self,s):  print "*ERR: "+s
    def debug(self,s):  print "DEBG: "+s

if __name__ == '__main__':
    # Hook used for stand-alone testing

    import sys
    if len(sys.argv) > 1: print sys.argv[1]

    #print " glob: " + `get_glob_list('/mnt/shared/AtCor/C1-AtCorProc/data/s2sim', '.dim')`

    #time string examples:
    #2011-12-29T00:00:00
    #2014-08-30T14:57:18Z
    #2014-08-30T14:57:18.123456Z

    ts = "2011-12-29T00:00:00"
    tp = TimePeriod(ts)
    print ts
    print "   " + `tp` + ",   " + `time.ctime(tp.begin_time)`
    print 

    ts = "2014-08-30T14:57:18Z"
    tp = TimePeriod(ts)
    print ts
    print "   " + `tp` + ",   " + `time.ctime(tp.begin_time)`
    print 
    
    ts = "2014-08-30T14:57:18.12345"
    tp = TimePeriod(ts)
    print ts
    print "   " + `tp` + ",   " + `time.ctime(tp.begin_time)`
    print 

    ts = "2014-08-30T14:57:18.123456Z"
    tp = TimePeriod(ts)
    print ts
    print "   " + `tp` + ",   " + `time.ctime(tp.begin_time)`
