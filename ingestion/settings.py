############################################################
#  Project: DREAM
#  Module:  Task 5 ODA Ingestion Engine 
#  Author:  Vojtech Stefka  (CVC)
#  Contribution: Milan Novacek   (CVC)
#
#    (c) 2013 Siemens Convergence Creators s.r.o., Prague
#    Licensed under the 'DREAM ODA Ingestion Engine Open License'
#     (see the file 'LICENSE' in the top-level directory)
#
# Django settings for the ingestion engine.
#
############################################################

import os
import sys
import logging
import json

DEBUG = True
TEMPLATE_DEBUG = False
IE_AUTO_LOGIN  = True

IE_N_WORKFLOW_WORKERS = 8

# set to 0 for no debugging
IE_DEBUG       = 2

#Ingestion Engine Constants
IE_PROJECT   = 'ingestion'
IE_HOME_PAGE = 'ingestion'

SC_NCN_ID_BASE     = 'scid'
NCN_ID_LEN         = 96
SC_NAME_LEN        = 256
SC_DESCRIPTION_LEN = 2048
SC_DSRC_LEN        = 1024
PROD_ERROR_LEN     = 2048

# Scripts are located in IE_SCRIPTS_DIR, defined further on down
#  Here use only the leaf file name, not the full path
IE_DEFAULT_INGEST_SCRIPT  = 'def_ingest.sh'
IE_DEFAULT_UQMD_SCRIPT    = 'def_uqmd.sh'
IE_DEFAULT_ADDPROD_SCRIPT = 'def_addProduct.sh'
IE_DEFAULT_DEL_SCRIPT     = 'def_delete.sh'
IE_DEFAULT_CATREG_SCRIPT  = 'cat_reg.sh'

UQMD_SUBDIR = 'uqmd_metadata'
ADDPRODUCT_SUBDIR = 'added_products'

PROJECT_DIR = os.path.dirname(__file__)

# ../ingestion_config.json contains:
# DownloadManagerDir and DM_MaxPortWaitSecs
#
# Example:
#
# {
#    "DownloadManagerDir" : "../ngEO-download-manager",
#    "DM_MaxPortWaitSecs": 44
# }

try:
    fp = open("ingestion_config.json", "r")
    config = json.load(fp)
    fp.close()
except Exception as e:
    print "warning: ingestion_settings.json not found, "+`e`
    print "         using defaults"
    config = {}

ADMINS = (
    # ('Your Name', 'your_email@example.com'),
)

# IE_SERVER_PORT: 
# String: used in dmcontroller for ingestion.
# Should be set according to where the ingestion
# engine is listening.  
# Please use a string ('8000'), not a number (8000).
# Once the ie is running and the user accesses some pages
# of the Ingestion Admin, the port will be set to
# request['SERVER_PORT'] when certain requests are processed.
# To disable re-setting according to the request SERVER_PORT,
#  it would be necessary to disable the method set_ie_port() in
#  dm_control.py
if "ie_server_port" in config:
    IE_SERVER_PORT = `config["ie_server_port"]`
else:
    IE_SERVER_PORT = '8000'

MANAGERS = ADMINS


# ------------------- Download Manager -----------------------------
# The download manager dir must be set in ingestion_config.json,
# or as DOWNLOAD_MANAGER_DIR  here.

# How often to query the DM for the status of a running DAR, seconds
DAR_STATUS_INTERVAL = 1.250

if "DownloadManagerDir" in config:
    DOWNLOAD_MANAGER_DIR = config["DownloadManagerDir"]
else:
    DOWNLOAD_MANAGER_DIR = "../ngEO-download-manager"

if DOWNLOAD_MANAGER_DIR == '':
    raise Exception ("Undefined DOWNLOAD_MANGER_DIR")

DOWNLOAD_MANAGER_CONFIG_DIR = os.path.join(DOWNLOAD_MANAGER_DIR,"conf")

# The DM configuration file name, incl. its full path prefix.
DM_CONF_FN = os.path.join(
    DOWNLOAD_MANAGER_CONFIG_DIR,
    "userModifiableSettingsPersistentStore.properties")

if "DM_MaxPortWaitSecs" in config:
    MAX_PORT_WAIT_SECS = config["DM_MaxPortWaitSecs"]
else:
    MAX_PORT_WAIT_SECS = 40

BASH_EXEC_PATH = "/bin/bash"

STOP_REQUEST = 'STOPPING'

# ------------------- Optional Off-line setting  -----------------------
# Can be used to reduce web traffic during rapid development cycles, or
# to develop / tune offline. Note for production it probably does not
# make much sense: while in general enabling this should not harm anyhing,
# the data volumes during ingestion are likely to be orders magnitude 
# larger and online access to the product facility is likely needed anyway.
# To enable, in ../ingestion_settings.json set "LocalJQueryUiURL" to
# a URL that serves js/jquery-ui-1.10.3.custom.min.js and
# css/smmothness/jquery-ui.css, e.g.:
#  "LocalJQueryUiURL": "http://127.0.0.1/offline/jquery-ui/"
# For details of how this is applied see static/templates/base.html.
#
if "LocalJQueryUiURL"  in config:
    JQUERYUI_OFFLINEURL = config["LocalJQueryUiURL"]
else:
    JQUERYUI_OFFLINEURL = ""



# ------------------- Other Django settings  ---------------------------
AUTHENTICATION_BACKENDS = ('django.contrib.auth.backends.ModelBackend',)

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        # Add 'postgresql_psycopg2', 'mysql', 'sqlite3' or 'oracle'.
        'NAME': os.path.join(PROJECT_DIR,'static/db/dream.db'),
        # Or path to database file if using sqlite3.
        # The following settings are not used with sqlite3:
        'USER': '',
        'PASSWORD': '',
        # HOST is Empty for localhost through domain sockets or
        #              '127.0.0.1' for localhost through TCP.
        'HOST': '',                      
        'PORT': '',    # Set to empty string for default.
    }
}

# Hosts/domain names that are valid for this site; required if DEBUG is False
# See https://docs.djangoproject.com/en/1.5/ref/settings/#allowed-hosts
ALLOWED_HOSTS = []

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# In a Windows environment this must be set to your system time zone.
TIME_ZONE = 'Europe/Brussels'

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# If you set this to False, Django will not format dates, numbers and
# calendars according to the current locale.
USE_L10N = True

# If you set this to False, Django will not use timezone-aware datetimes.
USE_TZ = False

# Absolute filesystem path to the directory that will hold user-uploaded files.
# Example: "/var/www/example.com/media/"
MEDIA_ROOT = os.path.join(PROJECT_DIR, 'media')

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash.
# Examples: "http://example.com/media/", "http://media.example.com/"
MEDIA_URL = '/media/'

IE_SCRIPTS_DIR = os.path.join(MEDIA_ROOT, 'scripts')

# Absolute path to the directory static files should be collected to.
# Don't put anything in this directory yourself; store your static files
# in apps' "static/" subdirectories and in STATICFILES_DIRS.
# Example: "/var/www/example.com/static/"
STATIC_ROOT = os.path.join(PROJECT_DIR, 'static')

# URL prefix for static files.
# Example: "http://example.com/static/", "http://static.example.com/"
STATIC_URL = '/static/'

# Additional locations of static files
STATICFILES_DIRS = (
    # Put strings here, like "/home/html/static" or "C:/www/django/static".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
)


# List of finder classes that know how to find static files in
# various locations.
STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
    'dajaxice.finders.DajaxiceFinder',
)


# Make this unique, and don't share it with anybody.
SECRET_KEY = '3(g1lm05ub!0tqkm59))o4og!9+cf67y)h!d$4es0qj$pq=-q+'

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.Loader',
    'django.template.loaders.app_directories.Loader',
    'django.template.loaders.eggs.Loader',
)

TEMPLATE_CONTEXT_PROCESSORS = ( # add because of DAJAX
    'django.contrib.auth.context_processors.auth',
    'django.core.context_processors.debug',
    'django.core.context_processors.i18n',
    'django.core.context_processors.media',
    'django.core.context_processors.static',
    'django.core.context_processors.request',
    'django.contrib.messages.context_processors.messages',
)


MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    # Uncomment the next line for simple clickjacking protection:
    # 'django.middleware.clickjacking.XFrameOptionsMiddleware',
)

ROOT_URLCONF = IE_PROJECT + '.urls'

# Python dotted path to the WSGI application used by Django's runserver.
WSGI_APPLICATION = IE_PROJECT + '.wsgi.application'

TEMPLATE_DIRS = (
    # Put strings here, like
    #  "/home/html/django_templates" or "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    #os.path.join(PROJECT_DIR, 'templates'),
    os.path.join(PROJECT_DIR, "static",'templates'),
)

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    # Uncomment the next line to enable the admin:
    'django.contrib.admin',
    # Uncomment the next line to enable admin documentation:
    #'django.contrib.admindocs',
    'dajaxice',
    'dajax',
    'jquery',
    IE_PROJECT
)

# The logging configuration. 
# See http://docs.djangoproject.com/en/dev/topics/logging for
# more details on how to customize the configuration.

# How many lines worth of log file to show in the browser initially.
# Settable by the user in the browser, then saved by the
# browser locally as cookie.
BROWSER_N_LOGLINES = 35
LOGGING_DIR = os.path.join(os.getcwd(), "logs")
LOGGING_FILE = os.path.join(LOGGING_DIR,"log")
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse'
        }
    },
    'formatters': {
        'user_formatter': {
            'format': '%(levelname)s %(asctime)s %(module)s %(message)s'
        }
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler'
        },
        'file': {
            'level':'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'maxBytes' : 1000000,
            'backupCount' : 1,
            'filename': LOGGING_FILE,
            'formatter':'user_formatter'
        }
    },
    'loggers': {
        'dajaxice': {
            'handlers': ['console'],
            'level': 'ERROR',
            'propagate': True,
        },
        'dream.file_logger': {
            'handlers': ['file','console'],
#            'level': 'INFO',
            'level': 'DEBUG',
        }
    }
}

def set_autoLogin(auto):
    global IE_AUTO_LOGIN
    IE_AUTO_LOGIN = auto

