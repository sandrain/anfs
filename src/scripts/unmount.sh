#!/bin/sh

## unmount any osds if mounted
for osd in `mount | grep osd | awk '{print $1}'`; do
	umount $osd
done

