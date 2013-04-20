#!/bin/bash

for f in `ls *.dat`
do
    if ! [[ -s $f ]]
    then
        rm $f
    fi
done
