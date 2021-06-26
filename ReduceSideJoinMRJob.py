#! /usr/bin/python3
# ReduceSideJoinMRJob.py

from mrjob.job import MRJob

ORDER_HEADER = ('OrderID|CustomerID|EmployeeID|OrderDate|RequiredDate|ShippedDate|ShipVia|'
                'Freight|ShipName|ShipAddress|ShipCity|ShipRegion|ShipPostalCode|ShipCountry')
ORDER_DETAILS_HEADER = 'OrderID|ProductID|UnitPrice|Quantity|Discount'
FIELD_SEP = '|'

class ReduceSideJoinJob(MRJob):

    def mapper(self, _, line):
        
        # Skip the header lines in both files
        if line != ORDER_HEADER and line != ORDER_DETAILS_HEADER:
            fields = line.split(FIELD_SEP)
            if len(fields) == 5: # We have the order-details dataset
                key = int(fields[0]) # The key is in the attribute OrderID
                value = round(float(fields[2]) * int(fields[3]),2) # UnitPrice times Quantity
                yield key, ('DE', value)
            elif len(fields) == 14: # We have the order dataset
                key = int(fields[0]) # The key is in the attribute OrderID
                date = fields[3][:10] # We only want the date, so take the first ten characters
                country = fields[13]
                value = (date,country)
                yield key, ('OR', value)
            else:
                raise ValueError('An input file does not contain the required number of fields.')

    def reducer(self, key, values):

        total = 0
        count = 0
        for value in list(values):
            if value[0] == 'OR':
                date = value[1][0]
                country = value[1][1]
            elif value[0] == 'DE':
                count += 1
                total += value[1]
        yield key, (date, country, total, count)


if __name__ == '__main__':
    ReduceSideJoinJob.run()