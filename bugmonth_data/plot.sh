#!/bin/bash

for fname in `ls *.dat`
do
    plotname=`echo $fname | sed s/dat$/jpg/`
    colname=`echo $fname | sed s/\.dat$// | sed s/_/\ /g`
    R --no-save > /dev/null <<-EOF
        jpeg("graphs/${plotname}")
        x = scan(file="${fname}")
        hist(x, xlab="${colname}", main="Histogram of ${colname}")
        dev.off()
EOF
done
