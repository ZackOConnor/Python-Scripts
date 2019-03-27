import os
import re
import pandas as pd 
import pyodbc as db
import datetime
import time
import numpy as np
import pickle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report

def sql_conn(sql_connection):
    #Take in conn_dict as a dict of server, database, and driver info needed for SQL connection
    conn_credit = ('Trusted_Connection=yes;''DRIVER='+ sql_connection['driver']+ ';SERVER='+ sql_connection['server']+ ';PORT=1433;DATABASE='+ 
                   sql_connection['database'])
    conn = db.connect(conn_credit)
    return conn


conn_dict = {
    'server': 'P-VM-UTIL01',
    'database': 'Syspro',
    'driver': '{ODBC Driver 13 for SQL Server}'
}

#prep import table for new data
conn = sql_conn(conn_dict)
curs = conn.cursor()
curs.execute("use Syspro")
curs.execute("delete from Freight")
curs.commit()
curs.close()

class Carrier():

    def __init__(self, carrier_file_path):
        #take in carrier file path and split out carrier. Also house possiable column header names dict
        self.carrier_file_path = carrier_file_path
        self.carrier = self.carrier_file_path.split("\\")[(len(self.carrier_file_path.split("\\"))-1)]
        self.column_name_dict = {
            "cus_name"         : ["Name", "Consignee Name","Consignee","Customer Name","Customer","CONSIGNEE NAME",
                                  "CONSIGNEE", "CUSTOMER NAME","CUSTOMER", " Cons Name", "Cons Name", "CUSTOMER ", 
                                  "Customer "],
            "dis_center_state" : ["state","STATE","ST", "st", "State","Consignee State", "Cons State", " Cons State",
                                  " Cons St", "St"],
            "ship_date"        : ["Document Date" ,"Ship date", "Shipment Date", "SHIPDATE","Ship Date", "SHIP DATE", 
                                  "PICKUPDATE", "Pick Up Date", "PICKUP", "Pick Up", "Pickup Date", 
                                  "Actual Pickup Date", "SHIPDATE", "SHIP DATE ", "Pick Up ", " Pick Up", 
                                  " Pickup Date", " Ship Date", "Pick up date "],
            "pro#"             : ["Assignment", "Bill Number", "Invoice Number", "PRONO","Pro Number", " Pro Number", 
                                  "Pro #", "Pro#", "PRO#", "PRO #", "PRO Number", "OD Pro#", "Prono", "PRO", "PRO ", 
                                  " Pro Nbr"],
            "cost"             : ["Balance Due" ,"Amount DC", "Amount Due"," Invoice Amt", "Gross Amount", 
                                  "Cost","Total", "Charges", "Cost ", "COST ", "COST", "Costs", "Total Cost", 
                                  "Total Charge (Net)"],
            "accessorials"     : ["accessorials", "Accessorials", "Accessorial", "Acc.", "Accessorial Charge", 
                                  " TTL Acc Chgs", "Acc Chgs", "TTL Acc Chgs"]
        }

    def import_files(self, excel_sheet):
        #take in target excel sheet name and loop through all carrier files to pull out targeted data
        import_list = os.listdir(self.carrier_file_path)
        for file in import_list:
            #make sure it is the right file type
            if file.split(".")[len(file.split("."))-1] != "xlsx":
                continue
            #try and except for other common possiable sheet names if the target doesn't work, then read into a pandas
            #dataframe
            try:
                import_data = pd.read_excel(self.carrier_file_path + "\\" + file, sheet_name = excel_sheet)
            except:
                try:
                    import_data = pd.read_excel(self.carrier_file_path + "\\" + file, sheet_name = "Sheet1")
                except:
                    try:
                       import_data = pd.read_excel(self.carrier_file_path + "\\" + file) 
                    except:
                        print("Incorrect Sheet Name: ", file)
            #set up dict to hold ready to be imported data            
            import_dict = {
                "cus_name" : "",
                "dis_center_state" : "", 
                "ship_date" : "",
                "carrier" : self.carrier,
                "pro#" : "",
                "cost" : 0,
                "accessorials" : 0
            }
            import_data_list = list(import_data)
            #loop through columns and set the import dicts keys 
            for col in import_data_list: 
                for item, value in self.column_name_dict.items():
                    if col in value:
                        import_dict[item] = col

            row_counter = 1
            for index,row in import_data.iterrows():
                #special case for NCS carrier. Walgreens is the only retailer NCS shippes to and doesn't give retailer
                #data in their reports
                #builds a list of values in the needed order to be inserted in to SQL
                if self.carrier == "NCS":
                    if import_dict["accessorials"] != 0:
                        sql_import_list = ["Walgreens","NA",row[import_dict["ship_date"]],import_dict["carrier"],row[import_dict["pro#"]],row[import_dict["cost"]],row[import_dict["accessorials"]]]
                    else:
                        sql_import_list = ["Walgreens","NA",row[import_dict["ship_date"]],import_dict["carrier"],row[import_dict["pro#"]],row[import_dict["cost"]],0]

                else:
                #For every other carrier
                    if import_dict["accessorials"] != 0:
                        sql_import_list = [row[import_dict["cus_name"]],row[import_dict["dis_center_state"]],row[import_dict["ship_date"]],import_dict["carrier"],row[import_dict["pro#"]],row[import_dict["cost"]],row[import_dict["accessorials"]]]
                    else:
                        sql_import_list = [row[import_dict["cus_name"]],row[import_dict["dis_center_state"]],row[import_dict["ship_date"]],import_dict["carrier"],row[import_dict["pro#"]],row[import_dict["cost"]],0]

                #needed to help handle data quality issues around data formats
                if type(sql_import_list[2]) is not str:
                    try:
                        sql_import_list[2] = sql_import_list[2].strftime('%m/%d/%Y')
                    except:
                        pass

                #Removes all numeric charaters, 
                if type(sql_import_list[4]) is str:
                    sql_import_list[4] = re.sub("[^0-9^.]", "", sql_import_list[4])

                conn = sql_conn(conn_dict)
                curs = conn.cursor()
                curs.execute("use Syspro")

                try:
                    curs.execute("insert into Freight ( cus_name, dis_center_state, ship_date, carrier, pro#, cost, accessorials) values (?,?,?,?,?,?,?)", sql_import_list)
                except:
                    pass

                curs.commit()
                curs.close()
                #there will never be more then 2000 records in a file, but carriers excel files will cause the for loop
                #to run until the row limit with in excel.
                row_counter = row_counter + 1
                if row_counter > 2000:
                    break 

#run each carrier throught the data scraper and labler
XPO_Logisctics = Carrier("J:\\Freight Info\\Freight Payment\\XPO Logistics")
XPO_Logisctics.import_files("Orders")
NCS = Carrier("J:\\Freight Info\\Freight Payment\\NCS")
NCS.import_files("Orders")
YRC_Frieght = Carrier("J:\\Freight Info\\Freight Payment\\YRC Freight")
YRC_Frieght.import_files("Orders")
UPS_Freight = Carrier("J:\\Freight Info\\Freight Payment\\UPS Freight")
UPS_Freight.import_files("Orders")
OldD = Carrier("J:\\Freight Info\\Freight Payment\\OldD")
OldD.import_files("Orders")
NEMF = Carrier("J:\\Freight Info\\Freight Payment\\NEMF")
NEMF.import_files("Orders")
Estes = Carrier("J:\\Freight Info\\Freight Payment\\Estes")
Estes.import_files("Orders")
FedEx = Carrier("J:\\Freight Info\\Freight Payment\\FedEx Freight")
FedEx.import_files("Orders")

#Remove invalid pro numbers
conn = sql_conn(conn_dict)
curs = conn.cursor()
curs.execute("use Syspro")
curs.execute("delete from Freight where [pro#] < 1")
curs.commit()
curs.close()

#load the traned and pickled model
lable_model = pickle.load(open('C:\\Users\\zoconnor\\Desktop\\FrieghtLabels\\RandomForest.py', 'rb'))
conn = sql_conn(conn_dict)
curs = conn.cursor()

sql_query = 'SELECT [cus_name],[dis_center_state],[ship_date],[carrier],[pro#],[cost],[accessorials]FROM [Syspro].[dbo].[Freight]'

#Clean the data and run it through the labler 
frieght_df = pd.read_sql(sql_query, sql_conn)
waiting_predict_df = frieght_df
frieght_df['cus_name'] = frieght_df['cus_name'].str.replace('[^a-zA-Z]','')
frieght_features = pd.get_dummies(frieght_df)
features = np.array(frieght_features)
predictions = lable_model.predict(features)

count = 0
#Insert the lables 
for index, row in waiting_predict_df:
    sql_col_list = [predictions[count], row['cus_name'], row['dis_center_state'],row['ship_date'],row['carrier'],row['pro'],row['cost'],row['accessorials']]
    curs.execute('update Freight set fk_cus_name = ? where [cus_name] = ? and [dis_center_state] = ? and [ship_date] = ? and [carrier] = ? and [pro#] = ? and [cost] = ? and [accessorials]  = ?', sql_col_list)
    curs.commit()
    count = count + 1
curs.close()