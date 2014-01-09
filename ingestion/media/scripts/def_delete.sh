#!/usr/bin/env sh
# 
#  DREAM Delete script template.
#  This script is invoked by the Ingestion Engine
#  when a scenario is deleted.
#  It is expected that the script will de-register
#  products associated with the scenario.
#
# usage:
# $0 ncn_id
#
#  The script should exit with a 0 status to indicate
# success; a non-zero status indicates failure, and will
# prevent the scenario to be deleted from the Ingestion
# Engine's list of scenarios.
#

script_name="Default delete script"

echo $script_name "started."

if [[ $# < 1 ]]
then
    echo "Not enough args," $script_name "exiting with status 1."
    exit 1
fi

echo arg: $1
echo $script_name "finishing with status 0."
exit 0
