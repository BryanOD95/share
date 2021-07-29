#! /usr/bin/python3
# SummariseMRJob.py

from mrjob.job import MRJob, MRStep

ORDER_HEADER = ('OrderID|CustomerID|EmployeeID|OrderDate|RequiredDate|ShippedDate|ShipVia|Freight|'
                'ShipName|ShipAddress|ShipCity|ShipRegion|ShipPostalCode|ShipCountry')
ORDER_DETAILS_HEADER = 'OrderID|ProductID|UnitPrice|Quantity|Discount'

class SummariseJob(MRJob):

    def steps(self):
        return [
            MRStep(mapper = self.join_mapper,
                   reducer = self.join_reducer),
            MRStep(reducer = self.summary_reducer)
        ]

    def join_mapper(self, _, line):
        if line != ORDER_HEADER and line != ORDER_DETAILS_HEADER:
            fields = line.split('|')
            if len(fields) == 5:  # order-details dataset
                key = int(fields[0]) # Key is in attribute OrderID
                value = (float(fields[2]), int(fields[3])) # Value is the tuple UnitPrice and Quantity
                yield key, ('DE', value)
            elif len(fields) == 14: # order dataset
                key = int(fields[0]) # Key is in attribute OrderID
                year = fields[3][:4] # take only the first four characters
                yield key, ('OR', year)
            else:
                pass # TODO handle error

    def join_reducer(self, key, values):

        for value in list(values):
            if value[0] == 'OR':
                year = value[1]
            elif value[0] == 'DE':
                unitprice = value[1][0]
                quantity = value[1][1]

        yield year, (unitprice, quantity)

    def summary_reducer(self, key, values):

        values = list(values)
        unitprices = [v[0] for v in values]
        quantities = [v[1] for v in values]

        def median(vals):
            val_sort = sorted(vals)
            n = int(len(val_sort) / 2)
            if len(vals) % 2:
                return (val_sort)[n-1:n+1]
            else:
                return val_sort[n]

        def mean(vals):
            return sum(vals)/len(vals)

        def variance(vals):
            mean = sum(vals) / len(vals)
            deviations = [mean ** 2 for i in vals]
            return (sum(deviations) / (len(deviations) - 1))

        def standard_deviation(vals):
            return variance(vals) ** 0.5

        def mode(vals):
            return max(set(vals), key=vals.count)

        yield key, (
            median(unitprices),
            mean(unitprices),
            standard_deviation(unitprices),
            mean(quantities),
            mode(quantities)
        )


if __name__ == '__main__':
    SummariseJob.run()