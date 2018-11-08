import pyodbc
import pandas as pd
import sqlite3 as db
import os

cn = pyodbc.connect('DSN=QuickBooks Data;')

sql = "SELECT ROOT.FullName, Root.ItemInventoryAssemblyLnItemInventoryRefFullName, Root.ItemInventoryAssemblyLnQuantity FROM ItemInventoryAssemblyLine ROOT UNOPTIMIZED"
sql2 = "SELECT FullName, (QuantityOnHand - QuantityOnSalesOrder) AS QuantityOnHand FROM ItemInventoryAssembly UNOPTIMIZED WHERE FullName Like '3-FG:%8%541%' AND QuantityOnHand < 0"
sql3 = "SELECT FullName, QuantityOnHand FROM ItemInventory UNOPTIMIZED"

data = pd.read_sql(sql,cn)
data2 = pd.read_sql(sql2,cn)
data3 = pd.read_sql(sql3,cn)

df = pd.DataFrame(data)
df2 = pd.DataFrame(data2)
df3 = pd.DataFrame(data3)

print(df2)

cn.close()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_filename = os.path.join(BASE_DIR,'sqllite.db')
con = db.connect(db_filename)

df.to_sql('ItemInventoryAssemblyLine',con, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None)
df2.to_sql('NegativeItem',con, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None)
df3.to_sql('InventoryPart',con,schema=None,if_exists='replace', index=True, index_label=None,chunksize=None)
con.close()



