#!/usr/bin/env sh
echo "default script started"
echo arg: $1
echo "arg1 contains:"
cat $1
sleep 1
echo "default script running"
sleep 1
echo "default script still running"
sleep 1
echo "default script finishing with status 3."
exit 3
