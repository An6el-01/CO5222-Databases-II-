import json
import pandas as pd
from DateHelper import DateHelper 
from ODS import ODS

class ParseJSON:
    def __init__(self):
        print("Building Parse JSON via Constructor")
        with open("SalesJSON.json") as f:
            self.data = json.load(f, encoding="ISO-8859-1",)
            self.sale = pd.json_normalize(data = self.data['Sale'])



    def parseJSON(self):
        print('Parsing JSON File')
        self.parseDates()
        


    def parseDates(self):
        print('\t Parsing JSON Internet Sale Dates')
        dates_df = pd.DataFrame(self.sale['DateOfSale'])
        dates_df['FullDate'] = pd.to_datetime(dates_df['DateOfSale'])
        dates_df = DateHelper.convertDateValues(self, dates_df)
        dates_df = dates_df.drop(columns=['DateOfSale'])
        ODS.DimInternetSaleDates = ODS.DimInternetSaleDates.append(dates_df)
        #print(ODS.DimInternetSaleDates_df.to_string())


        
