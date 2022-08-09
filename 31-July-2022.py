import mysql.connector as conn
import pandas as pd
import pymongo

########### Task 1

# Ques 1. Read this dataset in pandas , mysql and mongodb
fitbit = pd.read_csv(r"C:\Users\preet\Downloads\FitBit data.csv")

db_sql = conn.connect(host='localhost', user='root', passwd='1212')
# creating a cursor
cursor = db_sql.cursor()

# Establishing the connectivity with the MangoDB atlas server
client = pymongo.MongoClient("mongodb+srv://dhan:1212@cluster0.n0zji.mongodb.net/?retryWrites=true&w=majority")
# Selecting the mongotest database
db_mongo = client['mongotest']

# Ques 2. while creating a table in mysql dont use manual approach to create it  ,always use automation to create a
# table in mysql
# hint - use csvkit library to automate this task and to load a data in bulk in you mysql
fit_col = fitbit.columns
# Cleaning the columns
fit_col = [x.replace(' ', '').replace('?', '').replace(',', '_') for x in fit_col]

fitbit['ActivityDate'] = pd.to_datetime(fitbit['ActivityDate'])

fitbit.head()
# creating replacements dictionary to map python datatype to SQL datatypes
replace_dct = {
    'object': 'VARCHAR(20)',
    'float64': 'FLOAT',
    'int64': 'BIGINT',
    'datetime64[ns]': 'DATE',
    'timedelta64[ns]': 'VARCHAR(20)'}

# creating the columns names as per the SQL
sql_col = ', '.join('{} {}'.format(n, d) for (n, d) in zip(fit_col, fitbit.dtypes.replace(replace_dct)))

# Creating the query
q1 = 'DROP TABLE IF EXISTS assignment1.fitbit ; CREATE TABLE assignment1.fitbit (' + sql_col + ' )'

cursor.execute(q1)
cursor.close()
# loading the data into the SQL
q2 = 'INSERT INTO assignment1.fitbit (' + ', '.join('{} '.format(n) for n in fit_col) + ') VALUES '
db_sql = conn.connect(host='localhost', user='root', passwd='1212')
# creating a cursor
cursor = db_sql.cursor()
for i in range(len(fitbit)):
    a = str(tuple(fitbit.iloc[i])).replace("Timestamp(", '').replace(" 00:00:00')", "' ")
    query = q2 + a
    print(query)
    cursor.execute(query)
    db_sql.commit()
cursor.close()

# Ques 3. convert all the dates available in dataset to timestamp format in pandas and in sql you to convert it in date
# format

fitbit['ActivityDate'] = pd.to_datetime(fitbit['ActivityDate'])

# Ques 4 . Find out in this data that how many unique id's we have

print('{} unique ID'.format(fitbit['Id'].nunique()))

# Ques 5 . which id is one of the active id that you have in whole dataset
# Assuming the active person wil be the one who burns most calories.
df5 = pd.DataFrame(fitbit.groupby(["Id"])['Calories'].sum())
df5[df5['Calories'] == max(df5['Calories'])]

# Ques 6 . how many of them have not logged there activity find out in terms of number of ids
df6 = pd.DataFrame(fitbit[fitbit['LoggedActivitiesDistance'] == 0]['Id'].unique(), columns=['ID'])
print('{}  IDs have not logged their activity'.format(len(df6)))

# Ques 7 . Find out who is the laziest person id that we have in dataset
# again the laziest person will have least calories burnt
df7 = pd.DataFrame(fitbit.groupby(['Id'])['Calories'].sum())
df7[df7['Calories'] == min(df7['Calories'])]

# Ques 8 . Explore over an internet that how much calories burn is required for a healthy person and find out how
# many healthy person we have in our dataset

# defining healty as a person whoes intake calories and burn calories are almost same
# to account for both male and female we will consider 1500 as avg calories per day
fitbit['Daily_req'] = fitbit['Calories'].apply(lambda x: 'Y' if x > 1500 else 'N')

df8 = pd.DataFrame(fitbit.groupby('Id')['Daily_req'].count())
df8['cnt_Y'] = pd.DataFrame(fitbit[fitbit['Daily_req'] == 'Y'].groupby('Id')['Daily_req'].count())
# A person is healty if he atleast burns 1500 calories a day for 3/4th of the given record.
df8['%_active_days'] = round(100 * df8['cnt_Y'] / df8['Daily_req'], 0)
df8['is Healthy'] = df8['%_active_days'].apply(lambda x: 'Y' if x > 75 else 'N')
df8[df8['is Healthy'] == 'Y']
print('{} no of healthy person'.format(len(df8[df8['is Healthy'] == 'Y'])))

# Ques 9. how many person are not a regular person with respect to activity try to find out those
# assuming not regular as a person who hose active days are less than 50%

df9 = df8[df8['%_active_days'] < 50]
df9

# Ques 10 . who is the thired most active person in this dataset find out those in pandas and in sql both in python
# person with max avg calories burn witll be most active and so on
df10 = pd.DataFrame(fitbit.groupby(['Id'])['Calories'].mean()).reset_index()
df10['Rank'] = df10['Calories'].rank(method='dense', ascending=False).astype('int')
df10[df10['Rank'] == 3]
# in SQL

cursor = db_sql.cursor()
q10 = '''   DROP TABLE IF exists ASSIGNMENT1.tab1 ;
            DROP TABLE IF exists ASSIGNMENT1.tab12 ;

            CREATE TABLE assignment1.tab1
            (
                `Id` bigint
                , `Avg_cal` float
            ) ;
            INSERT INTO tab1
            SELECT Id 
                    , avg(Calories) 
            FROM assignment1.fitbit
            GROUP BY Id
            
            ; 
            CREATE TABLE assignment1.tab12
            (
                `Id` bigint
                , `Avg_cal` float
                , `rnk` INT
            ) ;
            INSERT INTO assignment1.tab12
            SELECT *
                    , DENSE_RANK() OVER (ORDER BY Avg_cal DESC) AS 'RNK'
            FROM assignment1.tab1
            
            ; SELECT * 
            FROM assignment1.tab12
            WHERE RNK = 3  '''
cursor.execute(q10)

# Ques 11 . who is the 5th most laziest person avilable in dataset find it out

df11 = df10
df11['Lazy_rnk'] = df11['Calories'].rank(method='dense').astype('int')
df11[df11['Lazy_rnk'] == 5]

# Ques 12 . what is a total acumulative calories burn for a person find out

df12 = pd.DataFrame(fitbit.groupby(['Id'])['Calories'].sum()).reset_index()
df12.rename(columns={'Calories': 'Total calories'}, inplace=True)
print(df12)

##################### Task 2

# 1 . load this data in sql and in pandas with a relation in sql
# 2 . while loading this data you don't have to create a table manually you can use any automated approach to create
# a table and load a data in bulk in table


# creating replacements dictionary to map python datatype to SQL datatypes
replace_dct = {
    'object': 'VARCHAR(50)',
    'float64': 'FLOAT',
    'int64': 'INT',
    'datetime64[ns]': 'DATE',
    'timedelta64[ns]': 'VARCHAR(50)'}

# Creating store table and loading the data

superstore = pd.read_excel(r"C:\Users\preet\Downloads\Superstore_USA.xlsx")
store_col = superstore.columns
# cleaning the columns
store_col = [x.replace('-', '_').replace(' ', '_') for x in store_col]
superstore['Product Name'] = superstore['Product Name'].apply(lambda x: x.strip()[0:20])
superstore['Product Base Margin'].fillna(superstore['Product Base Margin'].mean(), inplace=True)

# Query to create the table superstore
sql_superstore = ', '.join('\n{} {}'.format(m, n) for (m, n) in zip(store_col, superstore.dtypes.replace(replace_dct)))

query_superstore = '''DROP TABLE IF EXISTS assignment1.store ;
            CREATE TABLE assignment1.store (''' + sql_superstore + " )"
cursor.execute(query_superstore)
cursor.close()

# Starting the connection and creating a cursor
db_sql = conn.connect(host='localhost', user='root', passwd='1212')
cursor = db_sql.cursor()
# writing query to insert data into store table
q11 = ''' INSERT INTO assignment1.store VALUE '''
for i in range(len(superstore)):
    a = str(tuple(superstore.iloc[i])).replace('Timestamp(', '').replace(" 00:00:00')", "' ")
    query = q11 + a
    # superstore table is already loaded. Need not to elecute, as it takes longer time.
    # cursor.execute(query)
    # db_sql.commit()

# Creating return table and loading the data

ret = pd.read_excel(r"C:\Users\preet\Downloads\Superstore_USA.xlsx", sheet_name='Returns')
ret_col = ret.columns

# Starting the connection and creating a cursor
db_sql = conn.connect(host='localhost', user='root', passwd='1212')
cursor = db_sql.cursor()
# query to create a table Return
ret_col = [x.replace('-', '_').replace(' ', '_') for x in ret_col]
sql_return = ', '.join('{} {}'.format(m, n) for (m, n) in zip(ret_col, ret.dtypes.replace(replace_dct)))
query_return = ''' DROP TABLE IF EXISTS assignment1.return ;
                CREATE TABLE assignment1.return (''' + sql_return + ' )'

cursor.execute(query_return)
cursor.close()

db_sql = conn.connect(host='localhost', user='root', passwd='1212')
cursor = db_sql.cursor()
for i in range(len(ret)):
    a = str(tuple(ret.iloc[i]))
    query = '''INSERT INTO assignment1.return VALUE ''' + a
    # cursor.execute(query)
    # db_sql.commit()
cursor.close()

# Creating manager table and loading the data

manager = pd.read_excel(r"C:\Users\preet\Downloads\Superstore_USA.xlsx", sheet_name='Users')
manager_col = manager.columns
# Starting the connection and creating a cursor
db_sql = conn.connect(host='localhost', user='root', passwd='1212')
cursor = db_sql.cursor()
sql_manager = ', '.join('{} {}'.format(m, n) for (m, n) in zip(manager_col, manager.dtypes.replace(replace_dct)))
query_manager = '''DROP TABLE IF EXISTS assignment1.manager ;
                    CREATE TABLE assignment1.manager (''' + sql_manager + ' )'

cursor.execute(query_manager)
cursor.close()

db_sql = conn.connect(host='localhost', user='root', passwd='1212')
cursor = db_sql.cursor()
for i in range(len(manager)):
    a = str(tuple(manager.iloc[i]))
    query = '''INSERT INTO assignment1.manager VALUE''' + a
    # cursor.execute(query)
    # db_sql.commit()

# 3 . Find out how many return that we ahve recived and with a product id

ret['Status'].value_counts()
store_return = pd.merge(superstore, ret, on='Order ID', how='right')
store_return.isna().sum()

# product category wise return
a = pd.DataFrame(store_return.groupby(['Product Category'])['Product Category'].count())
a = a.rename(columns={'Product Category': 'ReturnCount'}).reset_index()
print(a)
# product sub category wise return
a = pd.DataFrame(store_return.groupby(['Product Category', 'Product Sub-Category'])['Product Sub-Category'].count())
a = a.rename(columns={'Product Sub-Category': 'ReturnCount'}).reset_index()
print(a)

# 4 . try  to join order and return data both in sql and pandas

db_sql = conn.connect(host='localhost', user='root', passwd='1212')
cursor = db_sql.cursor()
query4 = """
SELECT tab1.Product_Category, count(*) AS return_count
FROM assignment1.store AS tab1
INNER JOIN assignment1.return AS tab2
ON tab1.order_id = tab2.order_id
GROUP BY tab1.product_category
"""
cursor.execute(query4)
for a, b in cursor:
    print('"{}" product category has {} number of return.'.format(a, b))

# 5 . Try to find out how many unique customer that we have
superstore['Customer ID'].nunique()
query5 = """
SELECT COUNT(DISTINCT Customer_id) AS distinct_customer
FROM assignment1.store
"""
cursor.execute(query5)
for i in cursor:
    print('{} no of unique customer'.format(i))

# 6 . try to find out in how many regions we are selling a product and who is a manager for a respective region

store_manager = pd.merge(superstore, manager, on='Region', how='inner')
store_manager.groupby(['Product Category', 'Product Sub-Category']).agg({'Region': pd.Series.nunique}).reset_index()

db_sql = conn.connect(host='localhost', user='root', passwd='1212')
cursor = db_sql.cursor()

query5 = '''
SELECT product_category, Product_sub_category, COUNT(DISTINCT region) 
FROM assignment1.store
GROUP BY  product_category, Product_sub_category
'''
cursor.execute(query5)
for a, b, c in cursor:
    print('"{}" product category "{}" product sub category were sold in "{}" region'.format(a, b, c))

# 7 . find out how many different differnet shipement mode that we have and what is a percentage usablity of all the shipment mode with respect to dataset

print('{} are different shipment modes'.format(superstore['Ship Mode'].unique()))
a = pd.DataFrame(superstore['Ship Mode'].value_counts())
a['Usablity %'] = round(a['Ship Mode'] * 100 / len(superstore), 2)
print(a)

query7 = """
SELECT DISTINCT ship_mode
FROM assignment1.store
"""
cursor.execute(query7)
for i in cursor:
    print("{} is shipping mode".format(i))
query7 = """
SET @total_cnt = (SELECT COUNT(*) 
                FROM assignment1.store)

; SELECT ship_mode, (count(*) / @total_cnt) *100 AS usablity
FROM assignment1.store
GROUP BY Ship_Mode
"""
cursor.execute(query7)

for i, j in cursor:
    print('{} shipping mode has {} % useablity'.format(i, j))

# 8 . Create a new coulmn and try to find our a diffrence between order date and shipment date
superstore['Shipping time'] = (superstore['Ship Date'] - superstore['Order Date']).dt.days

query8 = """
SELECT DATEDIFF(ship_date , order_date) AS shipping_time
FROM store
"""
cursor.execute(query8)
for i in cursor:
    print(i)

# 9 . base on question number 8 find out for which order id we have shipment duration more than 10 days
superstore[superstore['Shipping time'] > 10]

query9 = """
SELECT order_id, DATEDIFF(ship_date, order_date) AS shipping_time
FROM assignment1.store 
HAVING shipping_time > 10
ORDER BY  shipping_time
"""
cursor.execute(query9)
for a, b in cursor:
    print(" Order ID {} has shipping time {} days".format(a, b))

# 10 . Try to find out a list of a returned order which sihpment duration was more then 15 days and find out that region manager as well

store_manager_return = pd.merge((pd.merge(store, manager, on='Region', how='left')), ret, on='Order ID', how='outer')
print(store_manager_return[
          (store_manager_return['Status'].isna() == False) & (store_manager_return['Shipping time'] > 15)])

query10 = """

SELECT tab1.order_id , DATEDIFF(Order_Date, Ship_Date) AS ship_time
FROM assignment1.store AS tab1
LEFT JOIN (assignment1.manager AS tab2, assignment1.return AS tab3) 
ON (tab1.Region = tab2.Region AND tab1.Order_ID = tab3.order_ID )
WHERE status = 'Return'
HAVING ship_time > 15 
"""

cursor.execute(query10)
for a, b in cursor:
    print(" Return Order ID {} has that shipping time more than {} days".format(a, b))

# 11 . Gorup by region and find out which region is more profitable

import numpy as np

print(superstore.groupby(['Region']).agg({'Profit': np.sum}).sort_values(by='Profit').iloc[0])

query11 = """
SELECT Region, sum(Profit) as tot_profit
FROM assignment1.store
group by region
"""
cursor.execute(query11)

for i, j in cursor:
    print('region {} has total profit {}'.format(i, j))

# 12 . Try to find out overalll in which country we are giving more didscount

print(superstore.groupby(['State or Province']).agg({'Discount': np.mean}).sort_values(by=['Discount'], ascending=
False).iloc[0])

query12 = """
SELECT  state_or_province, avg(discount) AS avg_discount
FROM assignment1.store
GROUP BY state_or_province
ORDER BY avg_discount DESC
"""
cursor.execute(query12)

for i, j in cursor:
    print('State {} has mean discount {}'.format(i , j))

# 13 . Give me a list of unique postal code
print("Unique Postal codes are {}".format(super['Postal Code'].unique()))

query13 = """
SELECT DISTINCT Postal_code
FROM assignment1.store
"""
cursor.execute(query13)
print('Unique postal code')
for i in cursor:
    print(i)

# 14 . which customer segement is more profitalble find it out .

print(superstore.groupby(['Customer Segment']).agg({'Profit': np.sum}).sort_values(by = 'Profit', ascending = False))


query14 = """
SELECT Customer_segment, sum(Profit) AS tot_profit
FROM assignment1.store
GROUP BY customer_segment
ORDER BY tot_profit DESC 
"""

cursor.execute(query14)
for i, j in cursor:
    print('{} customer segment and its total profit {}'.format(i , j))

# 15 . try to find out the 10th most loss making product catagory .
a = superstore.groupby(['Product Sub-Category']).agg({'Profit': np.sum}).sort_values(by = 'Profit')
a['rnk'] = a['Profit'].rank(method = 'dense').astype('int')
print(a[a['rnk'] == 10])

query15 = """
SELECT Product_sub_category, SUM(Profit) AS tot_profit
FROM assignment1.store 
GROUP BY product_sub_category
ORDER BY tot_profit
"""
cursor.execute(query15)
lst_product = []
lst_profit = []
for i , j in cursor:
    lst_product.append(i)
    lst_profit.append(j)

a = pd.DataFrame(data = {'product_category': lst_product, 'profit': lst_profit})
a['RNK'] = a['profit'].rank(method='dense').astype('int')
print(a[a['RNK']==10])


# 16 . Try to find out 10 top  product with highest margins

print(pd.DataFrame(superstore.groupby(['Product Sub-Category'])['Product Base Margin'].sum()).sort_values(by = "Product Base Margin", ascending = False).iloc[0:10])


query16 = """
SELECT product_sub_category, sum(product_base_margin) as Max_margin
FROM assignment1.store
GROUP BY product_sub_category
ORDER BY Max_margin DESC
LIMIT 10
"""

cursor.execute(query16)

for i , j in cursor:
    print('{} product category has margin {}'.format(i, j))


