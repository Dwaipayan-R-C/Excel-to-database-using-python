import pandas as pd
import pymongo
import json
import numpy as np

class xl_2_db():

    def mainfuc(self): 
        
        # defining the file location with None sheet number argument
        sheets_dict = pd.read_excel(r'D:\github\04.06.2020_xls2db_Bahubali\customer_db.xlsx', None)
        
        # looping through the excel file which is stored as a dictionary of items
        # Here key is the sheet names and values are the dataframes in each sheet
        for name, sheet in sheets_dict.items():
            df = sheet
            sheet_name = name
            fun_call = self.data_coll(df,sheet_name)


    def data_coll(self,df,sheet_name):

        # Mongo DB collection defining and conditioning the collection for each sheets
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient["Customer_Database"]

        if sheet_name == 'Customer Master - Corparate Rep':                
            mycol = mydb['customer_datasheet_1']
            mycol.drop()

        elif sheet_name == 'Active Customer':
            mycol = mydb['customer_datasheet_2']
            mycol.drop()
        # ..........................................................................................

        # checking the boolean condition of the key = On Hold in excel Sheet
        # looping through the dataframe and check for On Hold key
        # If yes, store the corresponding lit for that particular column in a_onhold
        for i_key,j_val in df.items():    
            if i_key == 'On Hold':
                a_onhold= j_val.values

        # looping through thye list [On Hold], if the value is found Y, then True, Else false and then appened in a list named bool_l
        bool_l = []
        for p in a_onhold:
            if p == 'Y':
                b_val = True
            else:
                b_val = False
            bool_l.append(b_val)

        # total number of rows in the corresponding sheet is stored in the variable of total_rows
        total_rows = df.shape[0]

        # redefining the key names or the headers of excel sheet as per Mongodb format
        df = df.rename(columns={'#': 'serialNumber', 'Addres' : 'address', 'Telephone 1.1' : 'contactTelephoneNumber1', 'Telephone 2.1' : 'contactTelephoneNumber2', 'Mobile Phone.1' : 'contactMobileNumber',
        'BP Code' : 'bpCode', 'BP Name' : 'bpName', 'Customer Status' : 'customerStatus', 'On Hold' : 'onHold', 'Telephone 1':'telephone1', 'Telephone 2': 'telephone2', 'Mobile Phone':'mobilePhone', 
        'E-Mail':'eMail',	'Payment terms' : 'paymentTerms', 'Days' : 'days', 'Internal Number' : 'internalNumber', 'Contact Person Name':'contactPersonName',
        'Contact Person EMail' : 'contactPersonEmail',	'BP Currency':'bpCurrency', 'Creation Date':'creationDate', 'Last Transaction Date': 'lastTransactionDate','Credit Limit':'creaditLimit', 'Account Balance': 'accountBalance'})


        list_key = []
        list_val = []

        # looping through each row 
        print(df)
        for i in range(0,total_rows):
            datadict = {}

            # for each row, looping through the dictionaary od items with loop_key denoting the header name and loop_val containg list of datas corresponding to the header
            for loop_key, loop_val in df.items():
                

                # check for days and internalNumbers to convert it to integer types
                # if loop_key == 'days'or loop_key == 'internalNumber' :
                #     if np.isnan(loop_val[i]):                
                #         datadict[loop_key] = {}
                #         datadict[loop_key] = loop_val[i]

                #     else:

                #         x_int = int(loop_val[i])
                #         datadict[loop_key] = {}
                #         datadict[loop_key] = x_int

                # # check for the following keys to convert it to string if not Null
                # elif loop_key == 'lastTransactionDate' or loop_key == 'creationDate':
                #     if pd.isnull(loop_val[i]):
                #         datadict[loop_key] = {}
                #         datadict[loop_key] = None

                #     else:
                #         date = loop_val[i]
                #         d_format = str(date._date_repr)

                #         datadict[loop_key] = {}
                #         datadict[loop_key] = d_format

                # # calling the boolean list appended in bool_l variable earlier
                # elif loop_key == 'onHold':
                #     datadict[loop_key] = {}
                #     datadict[loop_key] = bool_l[i]

                # # convert the numpy dtype 'int64' or 'float64' to native datatypes as Mongdb doesn't accept tyhe following
                # elif type(loop_val[i]) == np.int64:            
            
                #     x_int = int(loop_val[i])
                #     datadict[loop_key] = {}
                #     datadict[loop_key] = x_int

                # elif type(loop_val[i]) == np.float64:
                #     x_float = float(loop_val[i])
                #     datadict[loop_key] = {}
                #     datadict[loop_key] = x_float

                # # if the other values are not null, put it in string format
                # else:
                #     if pd.isnull(loop_val[i]):
                #         datadict[loop_key] = {}
                #         datadict[loop_key] = None
                    
                #     else:
                datadict[loop_key] = {}
                datadict[loop_key] = str(loop_val[i])
                print(datadict)
                        
            # update to the collection after one set of document is created
            x = mycol.insert_one(datadict)

obj_excel=xl_2_db()
obj_excel.mainfuc()
