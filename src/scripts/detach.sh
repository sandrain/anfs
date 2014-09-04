#!/bin/sh

## check if any osds attached
found=`ls /dev/ | grep osd`
if [ -z "$found" ]; then
	echo "No osd device found"
	exit 0
fi

## check if they are busy/mounted
found=`mount | grep /dev/osd`
if [ ! -z "$found" ]; then
	echo "osd devices seem to be mounted. unmount it first."
	exit 1
fi

## now safely logout all targets
iscsiadm -m node --logoutall=all

exit 0

