#!/bin/sh

servers="atom-a1,atom-a2,atom-b1,atom-b2,atom-c1,atom-c2,atom-d1,atom-d2"
#servers="atom-a1,atom-a2,atom-b1,atom-b2"
#servers="atom-a1,atom-a2"
spath="/ccs/techint/home/hs2/workspace/activefs/activeosd/osc-osd/"
PDSH="pdsh -w $servers"

$PDSH "pidof otgtd | xargs kill -9"
$PDSH "killall gdb"

