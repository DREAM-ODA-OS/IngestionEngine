#!/usr/bin/env sh
# 
#  DREAM Ingestion script template.
#  This script is invoked by the Ingestion Engine
#  to ingest a downloaded product into the ODA server.
#
# usage:
# $0 manifest-file [-catreg]
#
#  The script should exit with a 0 status to indicate
# success; a non-zero status indicates failure.
#
# catreg is a flag to request registration in the local
#        metadata catalogue.  If absent no registration
#        should be done.
#
# The manifest file is further input to this script,
# and contains KV pairs.  Values are strings enclosed
# in quotes. Here are examples of the most important
# KV pairs contained in the manifest file:
#
#    SCENARIO_NCN_ID="scid0"
#    DOWNLOAD_DIR="/path/p_scid0_001"
#    METADATA="/path/p_scid0_001/ows.meta"
#    DATA="/path/p_scid0_001/p1.tif"
#
#

echo "Default Ingestion script started."

if [[ $# < 1 ]]
then
    echo "Not enough args, exiting with status 1."
    exit 1
fi

if [[ $2 == '-catreg' ]]
then
    echo "Catalogue registration requested." 
fi

echo arg: $1
echo "arg1 contains:"
cat $1
echo "Default Ingestion script finishing with status 0."
exit 0
