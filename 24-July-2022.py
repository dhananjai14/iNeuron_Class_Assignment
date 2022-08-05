import mysql.connector as conn
import pandas as pd
import pymongo
# connecting to the SQL database
db = conn.connect(host='localhost', user='root', passwd='1212')
# creating a cursor
cursor = db.cursor()

# Establishing the connectivity with the MangoDB atlas server
client = pymongo.MongoClient("mongodb+srv://dhan:1212@cluster0.n0zji.mongodb.net/?retryWrites=true&w=majority")
# Selecting the mongotest database
db1 = client['mongotest']

# 1. Create a  table attribute dataset and dress dataset


attribute = pd.read_csv(r"C:\Users\preet\Desktop\DS\Data set\data fsds\Attribute DataSet.csv")
sales = pd.read_csv(r"C:\Users\preet\Desktop\DS\Data set\data fsds\Dress Sales.csv")
col_attributes = attribute.columns
col_sales = sales.columns

# cleaning the column names
col_attributes = [x.lower().replace(' ', '_').replace('?', '').replace('-', '_').
                  replace(')', '').replace('(', '').replace(' ', '_') for x in col_attributes]

col_sales = [x.lower().replace(' ', '_').replace('?', '').replace('-', '_').
             replace(')', '').replace('(', '').replace(' ', '_') for x in col_sales]

# %% creating the SQL query statements

# creating replacements dictionary to map python datatype to SQL datatypes
replace_dct = {
    'object': 'VARCHAR(20)',
    'float64': 'FLOAT',
    'int64': 'INT',
    'datetime64': 'TIMESTAMP',
    'timedelta64[ns]': 'VARCHAR(20)'
}

# table columns names in SQL
q1 = ', '.join('{} {}'.format(n, d) for (n, d) in zip(col_attributes, attribute.dtypes.replace(replace_dct)))
q2 = ', '.join('{} {}'.format(n, d) for (n, d) in zip(col_sales, sales.dtypes.replace(replace_dct)))
# Creating final query
q1 = ' DROP TABLE IF EXISTS assignment1.attribute ; CREATE TABLE assignment1.attribute (' + q1 + ')'
q2 = ' DROP TABLE IF EXISTS assignment1.sales ; CREATE TABLE assignment1.sales (' + q2 + ')'
cursor.execute(q1)
cursor.close()
db = conn.connect(host='localhost', user='root', passwd='1212')
cursor = db.cursor()
cursor.execute(q2)
cursor.close()
# 2. Do a bulk load for these two table for respective dataset

# Writing a query
db = conn.connect(host='localhost', user='root', passwd='1212')
cursor = db.cursor()
q3 = '''INSERT INTO assignment1.attribute (''' \
     + ', '.join('{} '.format(n) for (n) in col_attributes) \
     + ') VALUES '
# extracting the data from csv and inserting into DB table
attribute = attribute.fillna('NA')
for i in range(len(attribute)):
    a = tuple(attribute.iloc[i])
    query = q3 + str(a)
    cursor.execute(query)
    db.commit()

sales = sales.fillna(0)
sales = sales.replace('Removed', 0).replace('removed', 0).replace('Orders', 0)
sales.fillna(0, inplace = True)
for i in sales.columns:
    sales[i] = sales[i].astype('int')
    print(i)

# Inserting data in sales table
q3_1 = '''INSERT INTO assignment1.sales (''' \
        + ', '.join('{} '.format(n) for n in col_sales) \
        + ') VALUES '
print(q3_1)
for i in range(len(sales)):
    a = tuple(sales.iloc[i])
    query = q3_1 + str(a)
    cursor.execute(query)
    db.commit()

# 3. read these dataset in pandas as a dataframe

attribute = pd.read_csv(r"C:\Users\preet\Desktop\DS\Data set\data fsds\Attribute DataSet.csv")
sales = pd.read_csv(r"C:\Users\preet\Desktop\DS\Data set\data fsds\Dress Sales.csv")

# 4. Convert attribute dataset in json format
att = attribute.fillna('NA')
att = att.to_dict(orient='records')

# 5. Store this dataset into mongodb
# creating client
client = pymongo.MongoClient('mongodb+srv://dhan:1212@cluster0.n0zji.mongodb.net/?retryWrites=true&w=majority')
# connection to database
db1 = client['mongotest']
# creating collection
coll = db1['attribute']
coll.insert_many(att)

# 6. in sql task try to perform left join operation with attribute dataset and dress dataset on column Dress_ID

q6 = '''SELECT * FROM assignment1.attribute as tab1
        INNER JOIN assignment1.sales as tab2
        ON tab1.dress_id = tab2.dress_id'''
cursor.execute(q6)
for i in cursor:
    print(i)

# 7. Write a sql query to find out how many unique dress that we have based on dress id
q7 = '''SELECT DISTINCT(dress_id)
        FROM assignment1.attribute'''
cursor.execute(q7)
for i in cursor:
    print(i)

# 8. Try to find out how mnay dress is having recommendation 0

q8 = ''' SELECT count(*) as 'Dress with 0 recommendation'
        FROM assignment1.attribute
        WHERE recommendation = 0'''
cursor.execute(q8)
for i in cursor:
    print(' total of {} dress with 0 recommendation '.format(i[0]))

# 9. Try to find out total dress sell for individual dress id

q9 = '''SELECT dress_id
            , `29_08_13` + `31_08_13` + `09_02_13` + `09_04_13` + `09_06_13` + `09_08_13` + `09_10_13`  + `09_12_13`
            + `14_09_13` + `16_09_13` + `18_09_13` + `20_09_13` + `22_09_13` + `24_09_13` + `26_09_13` + `28_09_13`
            + `30_09_13` + `10_02_13` + `10_04_13` + `10_06_13` + `10_08_10` + `10_10_13` +	`10_12_13`  AS 'Total Sales'
            FROM assignment1.sales '''
cursor.execute(q9)
for i in cursor:
    print('Dress ID {}, Overall Sales {}'.format(i[0], i[1]))

# 10. Try to find out a third highest most selling dress id

q10 = '''   DROP TABLE IF EXISTS assignment1.temp_tab1 ;  
            CREATE TABLE assignment1.temp_tab1
            SELECT dress_id
                , `29_08_13` + `31_08_13` + `09_02_13` + `09_04_13` + `09_06_13` + `09_08_13` + `09_10_13`  + `09_12_13`
                + `14_09_13` + `16_09_13` + `18_09_13` + `20_09_13` + `22_09_13` + `24_09_13` + `26_09_13` + `28_09_13`
                + `30_09_13` + `10_02_13` + `10_04_13` + `10_06_13` + `10_08_10` + `10_10_13` +	`10_12_13`  AS 'Total Sales'
            FROM assignment1.sales ;
            
            DROP TABLE IF EXISTS assignment1.temp_tab2 ;
            CREATE TABLE assignment1.temp_tab2
            SELECT *
                   , dense_rank() OVER(ORDER BY `TOTAL SALES` DESC) AS `RNK`
            FROM assignment1.temp_tab1 ;

      '''

q10 = '''   SELECT *
            FROM assignment1.temp_tab2
            WHERE `RNK` = 3'''
cursor.execute(q10)
for i in cursor:
    print('Dress ID: {} , Sales: {}, Rank: {}'.format(i[0], i[1], i[2]))
