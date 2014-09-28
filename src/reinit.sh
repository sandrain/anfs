#!/bin/sh

targetsh="scripts/targets.sh"

if [ -n "$1" ]; then
        targetsh="scripts/targets-$1.sh"
        if [ ! -f "$targetsh" ]; then
                echo "$targetsh is not valid"
                exit 1
        fi

        ff=`basename $targetsh`
        cd scripts
        rm -f targets.sh
        ln -s $ff targets.sh
        cd ..
fi

umount ./mnt
for dir in /mnt/afe*; do umount $dir; done
rm -f /tmp/anfs.db

## read the target configuration
source $targetsh

. scripts/detach.sh
. scripts/serverdown.sh
. scripts/serverup.sh 1
. scripts/attach.sh

for osd in /dev/osd*; do
	i=`echo $osd | grep -o [0-9]`
	mkfs.exofs --pid=0x22222 --format --osdname=afe$i $osd
done

if [ -z "`lsmod | grep exofs`" ]; then
	modprobe -f exofs
fi

for osd in /dev/osd*; do 
	i=`echo $osd | grep -o [0-9]`
	mount -t exofs -o pid=0x22222 $osd /mnt/afe$i
done

mount | grep osd

yes | ./mkfs.sh

if [ ! -d ./mnt ]; then
	mkdir mnt
fi


