#! /usr/bin/python3
# MaxTemperatureReducer.py

import sys

current_year = None
max_temperature = None

for line in sys.stdin:
    line = line.rstrip("\n")
    year, air_temperature = line.split(",")
    if year == current_year:
        if int(air_temperature) > max_temperature:
            max_temperature = int(air_temperature)
    else:
        if current_year is not None:
            print('{},{}'.format(current_year, max_temperature))
        max_temperature = int(air_temperature)
        current_year = year
if current_year is not None:
    print('{},{}'.format(current_year, max_temperature))