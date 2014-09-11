#!/bin/sh

fresh="0"

if [ $# -gt 0 ]; then
	fresh="1"
fi

spath="/ccs/techint/proj/anFS/activeosd/osc-osd/"
PDSH="pdsh -w $targets"

if [ $fresh -eq 1  ]; then
	$PDSH "rm -rf /tmp/osdstore/*"
fi

$PDSH "cd $spath && ./up"

