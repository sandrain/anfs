#!/bin/bash

pid='0x22222'

## unmount any osds if mounted
./unmount.sh

## mount devices
for osd in /dev/osd*; do
	i=`echo $osd | grep -o [0-9]`
	mkfs.exofs --pid=0x22222 --format --osdname=afe$i $osd
	mount -t exofs -o pid=0x22222 $osd /mnt/afe$i
done

## display the result
mount | grep osd

