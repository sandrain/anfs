#!/bin/sh

PDSH="pdsh -w $targets"

$PDSH "pidof otgtd | xargs kill -9"
$PDSH "killall gdb"

