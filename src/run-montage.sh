#!/bin/sh

if [ -z "`mount | grep ^anfs`" ]; then
	echo "anfs is not mounted, mounting.."
	./anfs mnt
fi

echo "copying data files.."
t1=`date +%s.%N`
cp -r eval/m101-par mnt
t2=`date +%s.%N`

t=`echo $t2 - $t1 | bc`
echo "copying files took $t seconds"

/opt/anfs/bin/anfs-submit.sh mnt/m101-par/montage.job
sleep 3

logfile="/tmp/afsjobs/current"

echo
echo
tail -f $logfile

## this doesn't work
#
#while [ ! -f "$file" ]; do
#	inotifywait -qqt 2 -e create -e moved_to $(dirname $logfile)
#done
#
#tail -f $logfile

