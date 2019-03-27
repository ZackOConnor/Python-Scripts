import pyodbc as db

conn_dict = {
    'server': 'P-VM-UTIL01',
    'database': 'Syspro',
    'driver': '{ODBC Driver 13 for SQL Server}'
}

def sql_conn(sql_connection):
    #takes in a dict of ser, database, and driver information to be used in teh SQL connection
    conn_credit = ('Trusted_Connection=yes;''DRIVER='+ sql_connection['driver']+ ';SERVER='+ sql_connection['server']+ ';PORT=1433;DATABASE='+ 
                   sql_connection['database'])
    conn = db.connect(conn_credit)
    return conn

cost_of_goods = 348337 #Enter in what ever valued needed to be allocated
customer_list= [] 
conn = sql_conn(conn_dict)
cur = conn.cursor()
#Select all customer names
cur.execute(" select distinct [syspro_cus_name_short] from [dbo].[PNLreadyWithFrieght]")
rows = cur.fetchall()
conn.close()

for row in rows:
    if row[0] == None:
        continue

    #isolate customer name    
    cus_name = [row[0]]

    #set up and build a list of customer names
    if customer_list == []:
        customer_list = [cus_name]
    else:
        customer_list = customer_list + cus_name

#created placeholder vars for cost of goods and customer dict
cogs_total = 0
cus_dict = {}

for cus_names in customer_list:
    conn = sql_conn(conn_dict)
    cur = conn.cursor()

    #Check if is a list and select data to be inserted diffrently if it is or isn't 
    if isinstance(cus_names,list):
        cur.execute(" select [EntryValue] from [dbo].[PNLreadyWithFrieght] where [GLSectionCode] = 5000 and \
                      [syspro_cus_name_short] = '"+cus_names[0]+"' and [GLYear] = 2018")
    else:
        recus_names = cus_names.replace("'", "''",10)
        cur.execute(" select [EntryValue] from [dbo].[PNLreadyWithFrieght] where [GLSectionCode] = 5000 and \
                      [syspro_cus_name_short] = '"+recus_names+"' and [GLYear] = 2018")

    rows = cur.fetchall()
    conn.close()
    cus_total = 0

    for row in rows:
        #builds cogs for all other customers
        cus_total = cus_total + row[0]
    #completes total cogs
    cogs_total = cogs_total + cus_total

    #Check if is a list and select data to be inserted diffrently if it is or isn't 
    if isinstance(cus_names, list):
        cus_dict[cus_names[0]] = cus_total
    else:
        cus_dict[recus_names] = cus_total

for key in cus_dict.keys():

    #allocates cogs by customers in cus_dict
    alloted_total = (cus_dict.get(key)/cogs_total)*cost_of_goods
    conn = sql_conn(conn_dict)
    cur = conn.cursor()
    cur.execute(" insert into [dbo].[ManualAdjust]([EntryType],[GLCode],[TransactionDate],[AccountType],[GLSectionCode]\
                  ,[syspro_cus_name_short],[EntryValue],[ReportIndex1])values ('D','5000.000.00.101','05/05/2018','E', \
                  5000,'"+key+"',?,'COST OF SALES')",alloted_total)
    cur.commit()