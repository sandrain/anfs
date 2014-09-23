#!/bin/sh

if [ -z "`mount | grep ^anfs`" ]; then
	echo "anfs is not mounted, mounting.."
	./anfs mnt
fi

echo "copying data files.."
t1=`date +%s.%N`
cp -r eval/sipros mnt
t2=`date +%s.%N`

t=`echo $t2 - $t1 | bc`
echo "copying files took $t seconds"

/opt/anfs/bin/anfs-submit.sh mnt/sipros/sipros.job
sleep 3
logfile="/tmp/afsjobs/current"

echo
echo
tail -f $logfile

