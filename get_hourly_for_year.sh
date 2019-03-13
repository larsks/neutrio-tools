#!/bin/sh

year=$1

for month in {1..12}; do
	start="${year}-${month}-01"
	echo "getting one month of hourly samples start on $start"

	python csvexport.py --granularity hours --month \
		-o "${year}-${month}.csv" $start
done
