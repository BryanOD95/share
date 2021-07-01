#! /usr/bin/python3
# ReduceSideJoinMRJob.py

from mrjob.job import MRJob

PRODUCTS_HEADER = ('ProductID|ProductName|SupplierID|CategoryID|QuantityPerUnit|UnitPrice|UnitsInStock|UnitsOnOrder|ReorderLevel|Discontinued')
CATEGORIES_HEADER = 'CategoryID|CategoryName|Description|Picture'
FIELD_SEP = '|'

class ReduceSideJoinJob(MRJob):

    def mapper(self, _, line):
        
        # Skip the header lines in both files
        if line != PRODUCTS_HEADER and line != CATEGORIES_HEADER:
            fields = line.split(FIELD_SEP)
            if len(fields) == 4: # We have the category dataset
                key = int(fields[0]) # The key is in the attribute CategoryID
                categoryName = fields[1]
                description = fields[2]
                value =  (categoryName, description) # UnitPrice times Quantity
                yield key, ("CAT", value)
            elif len(fields) == 10: # We have the products dataset
                key = int(fields[3]) # The key is in the attribute OrderID
                productId = fields[0]
                productName = fields[1]
                quantityPerUnit = fields[4]
                unitPrice = fields[5]
                unitsInStock = fields[6]
                unitsOnOrder = fields[7]
                
                value = (productId, productName, quantityPerUnit, unitPrice, unitsInStock, unitsOnOrder)
                yield key, ('PROD', value)
            else:
                raise ValueError('An input file does not contain the required number of fields.')

    def reducer(self, key, values):

        for value in list(values):
            if value[0] == 'PROD':
                productId = value[1][0]
                productName = value[1][1]
                quantityPerUnit = value[1][2]
                unitPrice = value[1][3]
                unitsInStock = value[1][4]
                unitsOnOrder = value[1][5]
                
            elif value[0] == 'CAT':
                categoryName = value[1][0]
                description = value[1][1]
        yield key, (categoryName, description, productId, productName, quantityPerUnit, unitPrice, unitsInStock, unitsOnOrder)
        
if __name__ == '__main__':
    ReduceSideJoinJob.run()