#! /usr/bin/python3
# MaxTemperatureMapper.py

import sys

MISSING = 9999

for line in sys.stdin :
    line = line.rstrip("\n")
    year = line[15:19]
    if line[87] == '+':
      air_temperature = int(line[88:92])
    else:
      air_temperature = int(line[87:92])
    quality = line[92:93]
    if air_temperature != MISSING and quality in ['0','1','4','5','9']:
        print('{},{}'.format(year,air_temperature))