import pandas as pd
from ODS import ODS

class ParseCSV:
    def __init__(self):
        print("Building Parse CSV via Constructor")
        self.sales_df = pd.read_csv("SalesCSV.csv", encoding="ISO-8859-1")
        #print(self.sales_df)

    def parseCSV(self):
        print("Parsing CSV files")
        self.parseSales()
       

    def parseSales(self):
        print("\t Parsing CSV Sales")
        self.sales_df = self.sales_df.rename(columns={"sale": "StoreSaleID", "employee": "EmployeeFirstName", "date": "DateOfSale", "item": "ProductID", "quantity": "Quantity","total": "SaleTotal"})
        self.sales_df['DateOfSale'] = pd.to_datetime(self.sales_df['DateOfSale'])
        ODS.DimStoreSale = ODS.DimStoreSale.append(self.sales_df)
       
       #print(ODS.DimStoreSale.to_string())