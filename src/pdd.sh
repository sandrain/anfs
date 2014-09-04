#!/bin/sh

t1=`date +%s`
dd if=/dev/zero of=mnt/p1 bs=1M count=1024 conv=fsync &
dd if=/dev/zero of=mnt/p2 bs=1M count=1024 conv=fsync && t2=`date +%s`

echo $t2 - $t1 | bc

