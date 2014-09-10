#!/bin/sh

umount ./mnt
for dir in /mnt/afe*; do umount $dir; done
rm -f /tmp/anfs.db

scripts/detach.sh
scripts/serverdown.sh

