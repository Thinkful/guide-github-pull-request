#!/bin/bash
l=`df -h | wc -l`
count=1
for i in `df -h | tr -s " " | cut -d " " -f 5 | tail -n $((l-1)) | cut -d "%" -f 1` 
do
	((count++))
	if [ `df -h | tr -s " " | cut -d " " -f 1 | head -n $count | tail -n 1` != "tmpfs" ] 
then
	if [ $i -gt $2 ]
	then
		echo "CRITICAL,`df -h | tr -s " " | cut -d " " -f 1 | head -n $count | tail -n 1`,$i"
		
	
	elif [ $i -le $2 -a $i -ge $1 ]
	then
		echo "WARNING,`df -h | tr -s " " | cut -d " " -f 1 | head -n $count | tail -n 1`,$i"
	
	elif [ $i -lt $1 ]
	then
		echo "OK,`df -h | tr -s " " | cut -d " " -f 1 | head -n $count | tail -n 1`,$i"
	fi
fi
done


