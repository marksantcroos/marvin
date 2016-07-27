#!/bin/bash

CHUNKSIZE=$(expr `stat -c %s $1` / $2)
split -a 1 -b $CHUNKSIZE $1 $3
sleep $4
