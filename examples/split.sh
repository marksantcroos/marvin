#!/bin/bash

if test `uname` == "Darwin"; then
CHUNKSIZE=$(expr `stat -f %z $1` / $2)
else
CHUNKSIZE=$(expr `stat -c %s $1` / $2)
fi

split -a 1 -b $CHUNKSIZE $1 $3
sleep $4
