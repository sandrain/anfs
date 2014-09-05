#!/bin/bash
# job submission shortcut.

if [ $# -ne 1 ]; then
	echo "Usage: $0 <job file>"
	echo
	exit 1
fi

## find out the mount directory
mntpnt="`mount | grep ^anfs | awk '{print \$3}'`"
submit="$mntpnt/.submit"

if [ ! -f $submit ]; then
	echo "$submit not found"
	exit 1
fi

## get the job file
jobfile="$1"

if [ ! -f $jobfile ]; then
	echo "$jobfile is not a valid file"
	exit 1
fi

inode="`stat --printf='%i' $jobfile`"

## create output files
datapath=`cat $jobfile | grep ^datadir | cut -d\" -f2`
outfilelist=`cat $1 | grep "^[ \t]*output" | grep -Po "(?<=\")[^\",]+(?=\")"` 

for outfile in $outfilelist; do
	touch $mntpnt/$datapath/$outfile
done;

echo $inode > $submit


