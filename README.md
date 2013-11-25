IngestionEngine
===============

DREAM Ingestion Engine

This is a development version, not released.
Most features are expected not to be functional yet.

## Update 25.11.2013:

*  The interface to the ngEO Download Manager (v 0.5.4) is now partially
operational. Feedback to the user is still lagging. The IE+DM can now
actually download something, although the downloaded products are not
registered in the ODA server yet. For development and debugging only
the first 3 donwload URLs generated for a given scenario are downloaded.
A test-scnenario is pre-loaded in the db.

## Installation and Configuration
Note 1: the s/w is meant to be used by members of the DREAM consortium 
and ESA, therefore some components are not necessarily publicly available.

Note 2: The Ingestion Engine has been lightly tested to be
launched with django's development server.

0. Download the IngestionEngine, e.g. to a directory `oda/iedir`
0. Get the ngEO-download-manager (not publicly available), 
unpack to a directory next to `iedir`, eg. `oda/ngEO-download-manager`
0. Configure if needed. In general no configuration should be neccessary.
 The top-level Ingestion Engine directory 
(`iedir` in our example) contains the config file `ingestion_config.json`.
Three of the most fundamental settings are there; for more fine-tuning 
edit the variables in `settings.py` or in `dm_control.py`. 
Notes about the paths configured In `ingestion_config.json`: if
the path for `DownloadManagerDir` 
is not absolute, then it is taken relative 
to the containing direcotory ('oda' in our example). Similarly, 
if `DownloadDirectory` is not absolute, then it is taken relative 
to the media directory, as defined in `setttings.py`.  In our example
it would be 'oda/iedir/ingestion/media/_DownloadDirectory_/`  The
DownloadDirectory is created if it does not exist.
0. Start the Ingestion Engine via django's development server. The IE
 will start the Download Manager automatically:

    ```
    cd oda/iedir
    ./manage.py runserver <port>
    ```
_port_ is optional and specifies where the ingestion engine is listening.
As a django application the default is 8000.
A successfuly completed start-up is indicated by the following lines
being logged to the logfile and to stdout:

    ```
    INFO org.eclipse.jetty.server.AbstractConnector - Started @0.0.0.0:8082
    Port OK, waited 22.2 secs.
    ```

0. View the Ingestion Admin Client page in a browser:
    `http://127.0.0.1:8000/ingestion`

0. To shut down, use '^C'.  This will shut down the IE, and cause the
Download Manager to also be shut down if the original Ingestion Engine 
process is still running. If the Ingestion Engine crashed and was 
restarted but the DM kept running, then you'll need to kill the DM 
process separately.

## License

See the LICENSE for licensing conditions.  Basically, the development version is restricted to the DREAM consortium and ESA.
