#!/bin/bash

pwd
ls -l $1
CHUNKSIZE=$(expr `stat -f %z $1` / $2)
split -a 1 -b $CHUNKSIZE $1 $3
sleep $4
