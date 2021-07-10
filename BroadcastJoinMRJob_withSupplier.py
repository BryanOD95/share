#! /usr/bin/python3
# BroadcastJoinMRJob.py

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import unicodecsv

category_header = "CategoryID|CategoryName|Description|Picture"
product_header = "ProductID|ProductName|SupplierID|CategoryID|QuantityPerUnit|UnitPrice|UnitsInStock|UnitsOnOrder|ReorderLevel|Discontinued"
supplier_head = "SupplierID|CompanyName|ContactName|ContactTitle|Address|City|Region|PostalCode|Country|Phone|Fax|HomePage"
class BroadcastJoinMRJob(MRJob):

    # allow UTF-8 encoded strings in the output
    OUTPUT_PROTOCOL = TextProtocol

    categories = {}
    filter_criterion = {}
    category_fields = category_header.split("|")
    product_fields = product_header.split("|")
    category_data = {}

    def configure_args(self):
        super(BroadcastJoinMRJob, self).configure_args()
        filter_help = "Specify filter criterion"
        self.add_passthru_arg('-filter', help=filter_help)

    def steps(self):
        return [
            MRStep(mapper_init = self.product_category_mapper_initialise,
                   mapper=self.product_category_mapper)
        ]

    def product_category_mapper_initialise(self):
        with open("categories.csv", "rb") as category_input:
            try:
                sharedcache = unicodecsv.reader(category_input, delimiter="|")
                for category in sharedcache:
                    if category != category_header:
                        self.category_data[category[0]] =\
                            dict(zip(self.category_fields[1:-1],category[1:-1]))
            except Exception:
                pass
            
        with open("suppliers.csv", "rb") as supplier_input:
            try:
                sharedcache = unicodecsv.reader(supplier_input, delimiter="|")
                for supplier in sharedcache:
                    if supplier != supplier_header:
                        self.supplier_data[supplier[0]] =\
                            dict(zip(self.supplier_fields[1:],supplier[1:]))
            except Exception:
                pass

    def product_category_mapper(self, _, product):
        if product != product_header:
            product_values = product.split("|")
            product_data = dict(zip(self.product_fields,product_values))
            yield product_data["ProductID"], product_data["ProductName"]+'\t'+\
                  '\t'.join(self.category_data[product_data["CategoryID"]].values())+'\t'+\
                  '\t'.join(self.supplier_data[product_data["SupplierID"]].values())

if __name__ == '__main__':
    BroadcastJoinMRJob.run()