#!/bin/sh

umount ./mnt
for dir in /mnt/afe*; do umount $dir; done
rm -f /tmp/anfs.db

source scripts/targets.sh

. scripts/detach.sh
. scripts/serverdown.sh

