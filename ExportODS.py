import pyodbc
from ODS import ODS
import pandas as pd

class ExportODS:
	buildSchema = '''
    DROP TABLE IF EXISTS FactSale;
DROP TABLE IF EXISTS DimStoreLocation;
DROP TABLE IF EXISTS DimDeliveryLocation;
DROP TABLE IF EXISTS DimProduct;
DROP TABLE IF EXISTS DimCustomer;
DROP TABLE IF EXISTS DimInternetSaleDates;
DROP TABLE IF EXISTS DimStoreSaleDates;
DROP TABLE IF EXISTS DimStoreSale;
DROP TABLE IF EXISTS DimInternetSale;
DROP TABLE IF EXISTS DimEmployeeLink;
DROP TABLE IF EXISTS DimEmployee;

CREATE TABLE DimEmployee(
	StoreID nvarchar(250) PRIMARY KEY NOT NULL,
	EmployeeID nvarchar(250),
	FirstName nvarchar(250),
	LastName nvarchar(250),
	BirthDate date,
	EndDate date,
	EmailAddress nvarchar(250),
	Phone nvarchar(11),
	EmergencyContactPhone nvarchar(11),
	StatusEmployee nvarchar(15),
);

CREATE TABLE DimEmployeeLink(
	StoreSaleID int,
	StoreID nvarchar(250),
	EmployeeID nvarchar(250),
	JobTitle nvarchar(250),
	JobDescription nvarchar(500),
	Salaried bit,
	PayRate money,
	PayFrequencyID int,
	FrequencyName nvarchar(15),
	VacationHours int,
	SickLeaveAllowance int,
	SalesPersonFlag bit,
	CONSTRAINT pk_employeetLink PRIMARY KEY (StoreSaleID, StoreID)
);

CREATE TABLE DimInternetSale(
	InternetSaleID nvarchar(250) PRIMARY KEY NOT NULL,
	CustomerID nvarchar(250),
	CustomerEmail nvarchar(250),
	CustomerFirstName nvarchar(250),
	CustomerLastName nvarchar(250),
	DateOfSale date,
	SaleAmount money,
	SaleTax money,
	SaleTotal money,
	Quantity int,
	DateShipped date,
	ShippingType nvarchar(50),
	City nvarchar(250),
	StateProvince nvarchar(250),
	Country nvarchar(250),
	PostalCode nvarchar(250),
);

CREATE TABLE DimStoreSale(
	StoreSaleID int PRIMARY KEY NOT NULL,
	DateOfSale date,
	ProductID nvarchar(250),
	Quantity int,
	SaleAmount money,
	SaleTax money,
	SaleTotal money,
	StoreAddress nvarchar(250),
	StorePostCode nvarchar(10),
	StoreCity nvarchar(250),
	StoreStateProvince nvarchar(250),
	StoreCountry nvarchar(250),
	StorePhone nvarchar(11),
	EmployeeFirstName nvarchar(250),
	EmployeeLastName nvarchar(250),
	EmployeeJobTitle nvarchar(250),
);

CREATE TABLE DimCustomer(
	CustomerID nvarchar(250) PRIMARY KEY NOT NULL,
	CustomerEmail nvarchar(250),
	FirstName nvarchar(250),
	LastName nvarchar(250),
	CustomerType nvarchar(250),
	City nvarchar(250),
	StateProvince nvarchar(250),
	Country nvarchar(250),
	PostalCode nvarchar(250),
);

CREATE TABLE DimStoreSaleDates(
	StoreSaleDateID nvarchar(50) PRIMARY KEY,
	FullDate date,
	Day nvarchar(50),
	Month nvarchar(50),
	Year int,
	DayOfYear int,
	Quarter int
);

CREATE TABLE DimInternetSaleDates(
	InternetSaleDateID nvarchar(50) PRIMARY KEY,
	FullDate date,
	Day nvarchar(50),
	Month nvarchar(50),
	Year int,
	DayOfYear int,
	Quarter int
);

CREATE TABLE DimProduct(
	ProductID nvarchar(250) PRIMARY KEY NOT NULL,
	ProductPrice money,
	ProductDescription nvarchar(500),
	CategoryID nvarchar(25),
	ParentCategory nvarchar(15),
	CategoryDescription nvarchar(500),
	SafetyStockLvl int,
	StockDate date,
	Units int,
	ReOrderPoint int,
	SupplierPrice money,
	SupplierAddress nvarchar(250),
	SupplierPostCode nvarchar(10),
	SupplierCountry nvarchar(250),
	SupplierCity nvarchar(250),
	SupplierStateProvince nvarchar (250),
	SupplierPhone nvarchar(11)
);

CREATE TABLE DimDeliveryLocation(
	DeliveryID nvarchar(250) PRIMARY KEY NOT NULL,
	Country nvarchar(250),
	City nvarchar(250),
	Postalcode nvarchar(250)
);

CREATE TABLE DimStoreLocation(
	StoreID nvarchar(250) PRIMARY KEY NOT NULL,
	Country nvarchar(250),
	City nvarchar(250),
	PostalCode nvarchar(250)
);

Create TABLE FactSale(
	SaleID nvarchar(250) PRIMARY KEY NOT NULL,
	StoreSaleID int,
	InternetSaleID nvarchar(250),
	StoreID nvarchar(250),
	DeliveryID nvarchar(250),
	CustomerID nvarchar(250),
	ProductID nvarchar(250),
	InternetSaleDateID nvarchar(50),
	StoreSaleDateID nvarchar(50),
	Quantity int,
	SalesTotal money,
	FOREIGN KEY (StoreSaleID) REFERENCES DimStoreSale (StoreSaleID),
	FOREIGN KEY (InternetSaleID) REFERENCES DimInternetSale (InternetSaleID),
	FOREIGN KEY (StoreID) REFERENCES DimEmployee (StoreID),
	FOREIGN KEY (DeliveryID) REFERENCES DimDeliveryLocation (DeliveryID),
	FOREIGN KEY (CustomerID) REFERENCES DimCustomer (CustomerID),
	FOREIGN KEY (ProductID) REFERENCES DimProduct (ProductID),
	FOREIGN KEY (InternetSaleDateID) REFERENCES DimInternetSaleDates (InternetSaleDateID),
	FOREIGN KEY (StoreSaleDateID) REFERENCES DimStoreSaleDates (StoreSaleDateID)
);

'''
	def __init__(self):
		print("Building SQL Data Warehouse from schema")
		connectionString = "DRIVER={SQL Server};SERVER=sql2016.fse.network;DATABASE=db_2014162_assignment_warehouse;UID=user_db_2014162_assignment_warehouse;PWD=Lavender01!"
		self.conn = pyodbc.connect(connectionString)
		self.cursor = self.conn.cursor()


	def buildTables(self):
		print("\t Building tables from ODS into SQL Warehouse")
		self.cursor.execute(self.buildSchema)
		self.conn.commit()
		#print("done")

	def exportUsingCSV(self, table, dataframe, chunksize):
		rows = len(dataframe.index)
		current = 0 #Starting at row 0
		while current < rows:
			if rows-current < chunksize: #Determine rows split into chunks
				 stop = rows
			else:
				stop = current+chunksize
			CSV = dataframe.iloc[current:stop].to_csv(index=False, header=False, quoting=1, quotechar="'", line_terminator="),\n(")
			CSV = CSV[:-3] #Delete last three chars i.e., ",\n("
			values = f"({CSV}" #Convert CSV into a string
			SQL = f"\t \t INSERT INTO {table} VALUES {values}".replace("''", 'NULL') #Replace empties with NULL
			print(SQL)
			current = stop #Move to end of chunk
			self.cursor.execute(SQL) #Execute the SQL
			self.conn.commit()

	def exportODS(self):
		print("Exporting ODS to SQL")
		for table in ODS.tables:
			chunksize = 1
			df = getattr(ODS, f"{table}")
			print(f"\t Exporting to {table} from {table}_df with {Len(df.index)} rows")
			self.exportUsingCSV(table, df, chunksize)