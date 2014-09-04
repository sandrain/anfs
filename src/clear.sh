#!/bin/sh

umount ./mnt
for dir in /mnt/*; do rm -rf $dir/*; done
rm -f /tmp/afs.db

mkfs.exofs --pid=0x22222 --format --osdname=a0 /dev/osd0
mkfs.exofs --pid=0x22222 --format --osdname=a1 /dev/osd1
mkfs.exofs --pid=0x22222 --format --osdname=a2 /dev/osd2
mkfs.exofs --pid=0x22222 --format --osdname=a3 /dev/osd3

