#! /usr/bin/python3
# PearsonCorrelationMRJob.py

from mrjob.job import MRJob

class PearsonCorrelationMRJob(MRJob):

    DELIMITER = ","
    FILE_HEADER = "AT,AP,AH,AFDP,GTEP,TIT,TAT,TEY,CDP,CO,NOX"

    def mapper(self, _, row):
        if row != self.FILE_HEADER:
            columns = row.split(self.DELIMITER)
            col_count = len(columns)
            # output each pair combination of columns
            for i in range(col_count):
                for j in range(col_count):
                    yield (i,j), (float(columns[i]),float(columns[j]))

    def combiner(self, key, values):

        x = y = xsq = ysq = xy = 0.0
        n = 0

        for value in values:
            # item a) in theoretical background section
            x += value[0] 
            # item b) in theoretical background section
            y += value[1] 
            # item c) in theoretical background section
            xsq += value[0]**2
            # item d) in theoretical background section
            ysq += value[1]**2
            # item e) in theoretical background section
            xy += value[0] * value[1]
            # and the total count, as mentioned in the theoretical background
            n += 1
        yield key, (x,y,xsq,ysq,xy,n)

    def reducer(self, key, values):

        x = y = xsq = ysq = xy = 0.0
        n = 0

        # produce a grand total for each of the values calculated in the combiner
        for value in values:
            x += value[0]
            y += value[1]
            xsq += value[2]
            ysq += value[3]
            xy += value[4]
            n += value[5]

        # see equation 2
        numerator = xy - ((x * y) / n)
        denominator_l = xsq - (x ** 2) / n
        denominator_r = ysq - (y ** 2) / n
        denominator = (denominator_l * denominator_r) ** 0.5

        yield key, numerator / denominator

if __name__ == '__main__':
    PearsonCorrelationMRJob.run()