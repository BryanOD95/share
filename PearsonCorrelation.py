#! /usr/bin/python3
# PearsonCorrelation.py

import sys
from PearsonCorrelationMRJob import PearsonCorrelationMRJob

if __name__ == '__main__':
     correlations = {}
     args = sys.argv[1:]
     job = PearsonCorrelationMRJob(args)
     with job.make_runner() as runner:
          runner.run()
          dims = 0

          # The mapreduce job returns correlation for each pair of
          # variables in a separate output row. 

          for key, value in job.parse_output(runner.cat_output()):

              # We add these to a dictionary with the indices of the 
              # variable as the key and the correlation as the value.
              correlations[tuple(key)] = value

              # We also keep track of how many columns are in the data
              if key[0] > dims:
                dims = key[0]


          # add 1 because column index starts at zero and we want a count!
          # and 1 because we want a column and row for the variable names
          dims += 2

          # create the matrix as a list of lists
          corr_matrix = [[None] * dims for i in range(dims)]

          col_num = 1
          corr_matrix[0][0] = ""

          # insert the variable names into the first row and column of the matrix
          for column in PearsonCorrelationMRJob.FILE_HEADER.split(","):
               corr_matrix[col_num][0] = column.rjust(6, " ")
               corr_matrix[0][col_num] = column.rjust(6, " ")
               col_num += 1
          # insert the correlation values from the dictionary into the matrix
          for  key, value in correlations.items():
               corr_matrix[key[0]+1][key[1]+1] = str(round(value,2)).rjust(6, " ")

          # pretty print the matrix
          print('\n'.join(['\t'.join([str(cell) for cell in row]) for row in corr_matrix]))