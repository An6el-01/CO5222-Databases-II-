import pyodbc
import pandas as pd
from ODS import ODS

class ParseSQL:
    def __init__(self):
        print("Building ParseSQL via Constructor")
        connectionString = "DRIVER={SQL Server};SERVER=sql2016.fse.network;DATABASE=db_2014162_co5222_assignment;UID=user_db_2014162_co5222_assignment;PWD=Lavender01!"
        self.conn = pyodbc.connect(connectionString)
        self.cursor = self.conn.cursor()

    def parseSQL(self):
        print("Parsing SQL")
        self.parseStoreSaleDates()
        self.parseInternetSaleDates()
        self.parseStoreLocation()
        self.parseDeliveryLocation()
        self.parseCustomer()
        self.parseInternetSale()
        self.parseStoreSale()
        self.parseProduct()
        self.parseSales()
        self.parseEmployee()
        self.parseEmployeeLink()
       # self.parseProduct()
        #test = pd.read_sql_query("SELECT * FROM Customer", self.conn)
        #print(test.to_string())

    def parseStoreSaleDates(self):
        print("\t Parsing SQL Store Sale Dates")
        Storedates_df = pd.read_sql_query("SELECT DateOfSale FROM StoreSale", self.conn)
        Storedates_df['FullDate'] = pd.to_datetime(Storedates_df['DateOfSale'])
        Storedates_df["StoreSaleDateID"] = Storedates_df['FullDate'].dt.strftime('%Y%m%d') #instead of 'FullDate' use DateOfSale fro Fact TBL
        Storedates_df['Day'] = Storedates_df['FullDate'].dt.strftime('%d') 
        Storedates_df['Month'] = Storedates_df['FullDate'].dt.strftime('%B')
        Storedates_df['Year'] = Storedates_df['FullDate'].dt.strftime('%Y')
        Storedates_df['DayOfWeek'] = Storedates_df['FullDate'].dt.strftime('%A')
        Storedates_df['DayOfYear'] = Storedates_df['FullDate'].dt.strftime('%j')
        Storedates_df['Quarter'] = Storedates_df['FullDate'].dt.quarter
        Storedates_df = Storedates_df.drop(columns=['DateOfSale'])
        ODS.DimStoreSaleDates =ODS.DimStoreSaleDates.append(Storedates_df)
        ODS.DimStoreSaleDates.drop_duplicates(subset='StoreSaleDateID', keep='first',inplace=True)   
        
        #print(ODS.DimStoreSaleDates.to_string())

    def parseInternetSaleDates(self):
        print("\t Parsing SQL Internet Sale Dates")
        Internetdates_df = pd.read_sql_query("SELECT DateOfSale FROM InternetSale", self.conn)
        Internetdates_df['FullDate'] = pd.to_datetime(Internetdates_df['DateOfSale'])
        Internetdates_df["InternetSaleDateID"] = Internetdates_df['FullDate'].dt.strftime('%Y%m%d')
        Internetdates_df['Day'] = Internetdates_df['FullDate'].dt.strftime('%d')
        Internetdates_df['Month'] = Internetdates_df['FullDate'].dt.strftime('%B')
        Internetdates_df['Year'] = Internetdates_df['FullDate'].dt.strftime('%Y')
        Internetdates_df['DayOfWeek'] = Internetdates_df['FullDate'].dt.strftime('%A')
        Internetdates_df['DayOfYear'] = Internetdates_df['FullDate'].dt.strftime('%j')
        Internetdates_df['Quarter'] = Internetdates_df['FullDate'].dt.quarter
        Internetdates_df = Internetdates_df.drop(columns=['DateOfSale'])
        ODS.DimInternetSaleDates =ODS.DimInternetSaleDates.append(Internetdates_df)
        ODS.DimInternetSaleDates.drop_duplicates(subset='InternetSaleDateID', keep='first',inplace=True)   

        #print(ODS.DimInternetSaleDates.to_string())

    def parseStoreLocation(self):
        print("\t Parsing SQL Store Location")
        StoreLocation_df = pd.read_sql_query("SELECT StoreID, StoreCountry as Country, StoreCity as City, StorePostCode as PostalCode FROM Store", self.conn)
        ODS.DimStoreLocation = ODS.DimStoreLocation.append(StoreLocation_df)
        ODS.DimStoreLocation.drop_duplicates(subset='StoreID', keep='first',inplace=True)       

        #print(ODS.DimStoreLocation.to_string())

    def parseDeliveryLocation(self):
        print('\t Parsing SQL Delivery Location')
        DeliveryLocation_df = pd.read_sql_query('SELECT CustomerEmail as DeliveryID,Country,City,PostalCode FROM Customer', self.conn)
        ODS.DimDeliveryLocation = ODS.DimDeliveryLocation.append(DeliveryLocation_df)
        ODS.DimDeliveryLocation.drop_duplicates(subset='DeliveryID', keep='first',inplace=True)       

        #print(ODS.DimDeliveryLocation.to_string())

    def parseCustomer(self):
        print("\t Parsing SQL Customers")
        Customer_df = pd.read_sql_query('SELECT CustomerID,CustomerEmail,FirstName,SecondName as LastName,CustomerType,City,StateProvince,Country,PostalCode FROM Customer', self.conn)
        ODS.DimCustomer = ODS.DimCustomer.append(Customer_df)
        ODS.DimCustomer.drop_duplicates(subset='CustomerID', keep='first', inplace=True)
        
        #print(ODS.DimCustomer.to_string())

    def parseInternetSale(self):
         print("\t Parsing SQL Internet Sale")
         InternetSale_df = pd.read_sql_query('SELECT SaleID AS InternetSaleID, CustomerID, DateOfSale, SaleAmount, SalesTax, SaleTotal FROM InternetSale', self.conn)
         Customer_df = pd.read_sql_query('SELECT CustomerID,CustomerEmail,FirstName as CustomerFirstName,SecondName as CustomerLastName,City,StateProvince,Country,PostalCode FROM Customer', self.conn)
         InternetSaleItem_df = pd.read_sql_query('SELECT SaleID AS InternetSaleID,Quantity,DateShipped,ShippingType FROM InternetSaleItem', self.conn)
         InternetSale_df = pd.merge(InternetSale_df,InternetSaleItem_df, left_on='InternetSaleID', right_on='InternetSaleID', how='left')
         InternetSale_df = pd.merge(InternetSale_df,Customer_df, left_on='CustomerID', right_on='CustomerID', how='left')
         ODS.DimInternetSale = ODS.DimInternetSale.append(InternetSale_df)
         ODS.DimInternetSale.drop_duplicates(subset='InternetSaleID', keep='first', inplace=True)

         #print(ODS.DimInternetSale.to_string)
      

    def parseStoreSale(self):
        print("\t Parsing SQL Store Sale")
        SaleItem_df = pd.read_sql_query('SELECT SaleID AS StoreSaleID, ProductID, Quantity FROM SaleItem', self.conn) #done
        StoreSale_df = pd.read_sql_query('SELECT SaleID AS StoreSaleID, StaffID, DateOfSale, SaleAmount, SalesTax, SaleTotal, StoreID  FROM StoreSale', self.conn)
        Store_df = pd.read_sql_query('SELECT * FROM Store', self.conn) #done
        Employee_df = pd.read_sql_query('SELECT JobTitleID, StoreID, FirstName as EmployeeFirstName, LastName as EmployeeLastName FROM Employee', self.conn) #done
        Job_df = pd.read_sql_query('SELECT JobTitleID, JobTitle as EmployeeJobTitle FROM Job', self.conn)

        StoreSale_df = pd.merge(StoreSale_df, SaleItem_df, left_on='StoreSaleID', right_on='StoreSaleID', how='left')
        Employee_df = pd.merge(Employee_df, Job_df, left_on='JobTitleID', right_on='JobTitleID', how='left')
        Store_df = pd.merge(Store_df, Employee_df, left_on='StoreID', right_on="StoreID", how="left")
        StoreSale_df = pd.merge(StoreSale_df, Store_df, left_on='StoreID', right_on='StoreID', how='left')
        ODS.DimStoreSale = ODS.DimStoreSale.append(StoreSale_df)
        ODS.DimStoreSale.drop_duplicates(subset='StoreSaleID', keep='first', inplace=True)

        #print(ODS.DimStoreSale.to_string)

    def parseProduct(self):
        print("\t Parsing SQL Product")
        Category_df = pd.read_sql_query('SELECT * FROM Category', self.conn)
        Product_df = pd.read_sql_query('SELECT * FROM Product', self.conn)
        Supplier_df = pd.read_sql_query('SELECT * FROM Supplier', self.conn)
        Stock_df = pd.read_sql_query('SELECT * FROM Stock', self.conn)

        Product_df = pd.merge(Product_df, Category_df, left_on='CategoryID', right_on='CategoryID', how='left')
        Product_df = pd.merge(Product_df, Supplier_df, left_on='SupplierID', right_on='SupplierID', how='left')
        Product_df = pd.merge(Product_df, Stock_df, left_on='ProductID', right_on='ProductID', how='left')
        ODS.DimProduct = ODS.DimProduct.append(Product_df)
        ODS.DimProduct.drop_duplicates(subset='ProductID', keep='first', inplace=True)

        #print(ODS.DimProduct.to_string)

        #continue wit link tables and Fact Tables

    def parseSales(self):
        print("\t Parsing SQL Sales")

        Sale_df = pd.read_sql_query('SELECT SaleID as StoreSaleID, ProductID, Quantity FROM SaleItem', self.conn)
        StoreSale_df = pd.read_sql_query('SELECT StaffID AS SaleID, SaleID as StoreSaleID, DateOfSale, StoreID, SaleTotal FROM StoreSale', self.conn)
        InternetCustomer_df = pd.read_sql_query('SELECT CustomerID, CustomerEmail as DeliveryID FROM Customer', self.conn)
        InternetSale_df = pd.read_sql_query('SELECT SaleID as InternetSaleID, CustomerID, SaleTotal, DateOfSale FROM InternetSale', self.conn)
        InternetSaleItem_df = pd.read_sql_query('SELECT SaleID as InternetSaleID, ProductID, Quantity FROM InternetSaleItem', self.conn)
        StoreSale_df['FullDate'] = pd.to_datetime(StoreSale_df['DateOfSale'])
        StoreSale_df["DateID"] = StoreSale_df['FullDate'].dt.strftime('%Y%m%d') #instead of 'FullDate' use DateOfSale fro Fact TBL
        InternetSale_df['FullDate'] = pd.to_datetime(InternetSale_df['DateOfSale'])
        InternetSale_df["DateID"] = InternetSale_df['FullDate'].dt.strftime('%Y%m%d')
        Sale_df = pd.merge(Sale_df, StoreSale_df, left_on='StoreSaleID', right_on="StoreSaleID", how = 'left')
        InternetSale_df = pd.merge(InternetSale_df, InternetCustomer_df, left_on='CustomerID', right_on='CustomerID', how='left')
        InternetSaleItem_df = pd.merge(InternetSaleItem_df, InternetSale_df, left_on='InternetSaleID', right_on='InternetSaleID', how='left')
        Sale_df = pd.merge(Sale_df, InternetSaleItem_df, left_on='ProductID', right_on='ProductID', how='left')
        StoreSale_df = StoreSale_df.drop(columns=['DateOfSale'])
        StoreSale_df = StoreSale_df.drop(columns=['FullDate'])
        InternetSale_df = InternetSale_df.drop(columns=['DateOfSale'])
        InternetSale_df = InternetSale_df.drop(columns=['FullDate'])
        ODS.FactSale = ODS.FactSale.append(Sale_df)
       
        #print(ODS.FactSale.to_string())

    def parseEmployee(self):
        print("\t Parsing SQL Employee")

        Employee_df = pd.read_sql_query('SELECT EmployeeID, FirstName, LastName, BirthDate, Hiredate, EndDate, EmailAddress, Phone, EmergencyContactPhone, Status AS StatusEmployee, StoreID FROM Employee', self.conn)
        ODS.DimEmployee = ODS.DimEmployee.append(Employee_df)
        ODS.DimEmployee.drop_duplicates(subset="EmployeeID", keep='first', inplace=True)

        #print(ODS.DimEmployee.to_string())

    def parseEmployeeLink(self):
        print("\t Matching Employee To Link")

        EmployeeLink_df = pd.read_sql_query('SELECT SaleID as StoreSaleID, StoreID FROM StoreSale',self.conn)
        Employeetbl_df = pd.read_sql_query('SELECT EmployeeID, StoreID, JobTitleID FROM Employee', self.conn)
        Job_df = pd.read_sql_query('SELECT JobTitle, JobTitleID, JobDescription, Salaried, PayRate, PayFrequencyID, VacationHours, SickLeaveAllowance, SalesPersonFlag FROM Job', self.conn)
        PayFrequency_df = pd.read_sql_query('SELECT PayFrequencyID, FrequencyName FROM PayFrequency', self.conn)
        Job_df = pd.merge(Job_df, PayFrequency_df, left_on='PayFrequencyID', right_on='PayFrequencyID', how='left')
        Employeetbl_df = pd.merge(Employeetbl_df, Job_df, left_on='JobTitleID', right_on='JobTitleID', how='left')
        EmployeeLink_df = pd.merge(EmployeeLink_df, Employeetbl_df, left_on='StoreID', right_on='StoreID', how='left')

        ODS.DimEmployeeLink = ODS.DimEmployeeLink.append(EmployeeLink_df)
        ODS.DimEmployeeLink.drop_duplicates(subset='StoreSaleID', keep='first', inplace=True)

        #print(ODS.DimEmployeeLink.to_string())
        














