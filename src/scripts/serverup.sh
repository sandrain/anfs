#!/bin/sh

fresh="0"

if [ $# -gt 0 ]; then
	fresh="1"
fi

servers="atom-a1,atom-a2,atom-b1,atom-b2,atom-c1,atom-c2,atom-d1,atom-d2"
#servers="atom-a1,atom-a2,atom-b1,atom-b2"
#servers="atom-a1,atom-a2"
#servers="atom-a1"
spath="/ccs/techint/home/hs2/workspace/activefs/activeosd/osc-osd/"
PDSH="pdsh -w $servers"

if [ $fresh -eq 1  ]; then
	$PDSH "rm -rf /tmp/afe1/*"
fi

$PDSH "cd $spath && ./up"

