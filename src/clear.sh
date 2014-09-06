#!/bin/sh

umount ./mnt
for dir in /mnt/afe*; do umount $dir; done
rm -f /tmp/anfs.db

scripts/detach.sh
scripts/serverdown.sh
#scripts/serverup.sh 1
#scripts/attach.sh
#
#for osd in /dev/osd*; do
#	i=`echo $osd | grep -o [0-9]`
#	mkfs.exofs --pid=0x22222 --format --osdname=afe$i $osd
#done
#
#for osd in /dev/osd*; do 
#	i=`echo $osd | grep -o [0-9]`
#	mount -t exofs -o pid=0x22222 $osd /mnt/afe$i
#done
#
#mount | grep osd
#
#cd scripts && ./pathdb-init.sh && cd ..
#
#yes | ./mkfs.sh


