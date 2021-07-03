#! /usr/bin/python3
# ReduceSideMRJobEX.py

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

ORDER_HEADER = ('OrderID|CustomerID|EmployeeID|OrderDate|RequiredDate|ShippedDate|ShipVia|Freight|'
                'ShipName|ShipAddress|ShipCity|ShipRegion|ShipPostalCode|ShipCountry')
ORDER_DETAILS_HEADER = 'OrderID|ProductID|UnitPrice|Quantity|Discount'

class ReduceSideJobEx(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol # The reducer will output only the value, not the key

    # Allow the job to receive an argument from the command line
    # This argument will be used to specify the type of join
    def configure_args(self):
        super(ReduceSideJobEx, self).configure_args()
        self.add_passthru_arg(
            '--join_type',
            default = 'inner',
            choices=['inner', 'left_outer','right_outer'],
            help="The type of join"
        )

    def mapper(self, _, line):
        if line != ORDER_HEADER and line != ORDER_DETAILS_HEADER:
            fields = line.split('|')
            if len(fields) == 5: # order-details dataset
                key = int(fields[0]) # key is in attribute OrderID
                value = round(float(fields[2]) * int(fields[3]),2) # UnitPrice times Quantity
                # deliberately remove some orders
                # to demonstrate outer joins
                if key not in range(10900, 10905):
                    yield key, ('DE', value)
            elif len(fields) == 14: # order dataset
                key = int(fields[0]) # key is in attribute OrderID
                date = fields[3][:10] # take only the first ten characters
                country = fields[13]
                value = (date,country)
                # deliberately remove some order details
                # to demonstrate outer joins
                if key not in range(10810,10817):
                    yield key, ('OR', value)
            else:
                raise ValueError('An input file does not contain the required number of fields.')

    def reducer(self, key, values):

        total = 0
        count = 0
        order_tuples = []
        detail_tuples = []

        for value in list(values):
            relation = value[0]  # either 'OR' or 'DE'
            if relation == 'OR':  # orders data
                date = value[1][0]
                country = value[1][1]
                order_tuples.append((date, country))
            elif relation == 'DE':  # details data
                detail_tuples.append(value[1])
            else:
                pass  # TODO handle error
        if self.options.join_type == 'inner':
            if len(order_tuples) > 0 and len(detail_tuples) > 0:
                for order_values in order_tuples:
                    output = [key] + [o for o in order_values] + [details_values]
                    yield None, output
        elif self.options.join_type == 'left_outer':
            if len(order_tuples) > 0:
                for order_values in order_tuples:
                    if len(detail_tuples) > 0:
                        count = len(detail_tuples)
                        total = sum(detail_tuples)
                        output = [key] + [o for o in order_values] + [count, total]
                    else:
                        output = [key] + [o for o in order_values] + ['null','null']
                    yield None, output
        elif self.options.join_type == 'right_outer':
            if len(detail_tuples) > 0:
                count = len(detail_tuples)
                total = sum(detail_tuples)
                if len(order_tuples) > 0:
                    for order_values in order_tuples:
                        output = [key] + [o for o in order_values] + [count, total]
                        yield None, output
                else:
                    output = [key] + ['null','null'] + [count, total]
                    yield None, output

if __name__ == '__main__':
    ReduceSideJobEx.run()