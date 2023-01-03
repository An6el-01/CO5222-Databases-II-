import pandas as pd


class ODS:
   tables =[
	   'DimEmployee'
	   'DimEmployeeLink'
	   'DimInternetSale' #Done
	   'DimStoreSale' #Done
	   'DimStoreSaleDates'#Done
	   'DimInternetSaleDates' #Done
	   'DimCustomer'#Done
	   'DimProduct'#Done
	   'DimDeliveryLocation' #Done
	   'DimStoreLocation' #Done
	   'FactSale' #Done
	   ]

   DimEmployee = pd.DataFrame(columns = ['EmployeeID',
										   'FirstName',
										   'LastName',
										   'BirthDate',
										   'EndDate',
										   'EmailAddress',
										   'Phone',
										   'EmergencyContactPhone',
										   'StatusEmployee',
										   'StoreID'])
   
   DimEmployeeLink = pd.DataFrame(columns = ['StoreSaleID',
												'EmployeeID',
												'StoreID',
												'JobTitle',
												'JobDescription',
												'Salaried',
												'PayRate',
												'PayFrequencyID',
												'FrequencyName',
												'VacationHours',
												'SickLeaveAllowance',
												'SalesPersonFlag'])

   DimInternetSale = pd.DataFrame(columns = ['InternetSaleID',
												 'CustomerID',
												 'CustomerEmail',
												 'CustomerFirstName',
												 'CustomerLastName',
												 'DateOfSale',
												 'SaleAmount',
												 'SalesTax',
												 'SalesTotal',
												 'Quantity',
												 'DateShipped',
												 'ShippingType',
												 'City',
												 'StateProvince',
												 'Country',
												 'PostalCode'])

   DimStoreSale = pd.DataFrame(columns = ['StoreSaleID',
											 'DateOfSale',
											 'ProductID',
											 'Quantity',
											 'SaleAmount',
											 'SalesTax',
											 'SalesTotal',
											 'StoreAddress',
											 'StorePostCode',
											 'StoreCity',
											 'StoreStateProvince',
											 'StoreCountry',
											 'StorePhone',
											 'EmployeeFirstName',
											 'EmployeeLastName',
											 'EmployeeJobTitle'])

   DimCustomer = pd.DataFrame(columns =['CustomerID',
										  'CustomerEmail',
										  'FirstName',
										  'LastName',
										  'CustomerType',
										  'City',
										  'StateProvince',
										  'Country',
										  'PostalCode'])

   DimStoreSaleDates = pd.DataFrame(columns =['StoreSaleDateID','FullDate', 'Day', 'Month', 'Year','DayOfYear','Quarter', 'DayOfWeek'])

   DimInternetSaleDates = pd.DataFrame(columns =['InternetSaleDateID','FullDate', 'Day', 'Month', 'Year','DayOfYear','Quarter', 'DayOfWeek'])


   DimProduct = pd.DataFrame(columns =['ProductID',
										  'ProductPrice',
										  'ProductDescription',
										  'ParentCategory',
										  'CategoryID',
										  'CategoryDescription',
										  'SafteyStockLvl',
										  'StockDate',
										  'Units',
										  'Re-OrderPoint',
										  'SupplierPrice'
										  'SupplierAddress',
										  'SupplierPostCode',
										  'SupplierCountry',
										  'SupplierCity',
										  'SupplierStateProvince',
										  'SupplierPhone'])
   DimDeliveryLocation = pd.DataFrame(columns = ['DeliveryID','Country', 'City', 'PostalCode'])

   DimStoreLocation = pd.DataFrame(columns = ['StoreID','Country', 'City', 'PostalCode'])

   FactSale = pd.DataFrame(columns =[	'SaleID',
										'StoreSaleID',
										'InternetSaleID',
										'StoreID',
										'DeliveryID',
										'CustomerID',
										'ProductID',
										'DateID',
										'Quantity',
										'SalesTotal'])
