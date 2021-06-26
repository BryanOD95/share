#! /usr/bin/python3
# MaxTemperatureJob.py

from mrjob.job import MRJob

class MRMaxTemperatureJob(MRJob):

    def mapper(self, _, line):

        MISSING = 9999

        year = line[15:19]
        if line[87] == '+':
          air_temperature = int(line[88:92])
        else:
          air_temperature = int(line[87:92])
        quality = line[92:93]
        if air_temperature != MISSING and quality in ['0','1','4','5','9']:
            yield year, air_temperature

    def reducer(self, year, air_temperatures):
        yield year, max(air_temperatures)

if __name__ == '__main__':
    MRMaxTemperatureJob.run()