#!/bin/bash

pid='0x22222'

## unmount any osds if mounted
./unmount.sh

## mount devices
for i in `seq 0 3`; do
	dev="/dev/osd$i"
	dir="/mnt/afs$i"
	mount -t exofs -o pid=$pid $dev $dir
done

## display the result
mount | grep osd

