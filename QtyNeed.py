import pyodbc
import pandas as pd
import sqlite3 as db
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_filename = os.path.join(BASE_DIR,'sqllite.db')
con = db.connect(db_filename)

cursor1 = con.cursor()
sql = "SELECT FullName, QuantityOnHand FROM NegativeItem"
cursor1.execute(sql)

Items = {}

Items = dict([(row[0], row[1]) for row in cursor1.fetchall()])

df3 = pd.DataFrame()

for item, qty in Items.items():
    sql2 = """WITH RECURSIVE RPL (FullName, ItemInventoryAssemblyLnItemInventoryRefFullName, ItemInventoryAssemblyLnQuantity) AS
    (
       SELECT ROOT.FullName, Root.ItemInventoryAssemblyLnItemInventoryRefFullName, Root.ItemInventoryAssemblyLnQuantity
       FROM ItemInventoryAssemblyLine ROOT
       WHERE ROOT.FullName = ?
       UNION ALL
      SELECT PARENT.FULLNAME, CHILD.ItemInventoryAssemblyLnItemInventoryRefFullName, PARENT.ItemInventoryAssemblyLnQuantity*CHILD.ItemInventoryAssemblyLnQuantity
    FROM RPL PARENT, ItemInventoryAssemblyLine CHILD
    WHERE PARENT.ItemInventoryAssemblyLnItemInventoryRefFullName = CHILD.Fullname            
    )

    SELECT FullName, ItemInventoryAssemblyLnItemInventoryRefFullName,SUM(ItemInventoryAssemblyLnQuantity)*? AS "TotalQTYUsed"
    FROM RPL
    GROUP BY FullName, ItemInventoryAssemblyLnItemInventoryRefFullName
    ORDER BY FullName, ItemInventoryAssemblyLnItemInventoryRefFullName"""

    cursor2 = con.cursor()

    cursor2.execute(sql2,(item,-int(qty)))

    df2 = pd.DataFrame(cursor2.fetchall(),columns=['FullName','ItemInventoryAssemblyLnItemInventoryRefFullName','TotalQTYUsed'])

    print (item,qty)
    print (df2)
    print ("===========================")

    df3 = df3.append(df2, ignore_index=True)



df3.to_sql('Detail',con, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None)

df4 = df3.groupby('ItemInventoryAssemblyLnItemInventoryRefFullName').sum()

df4.to_sql('Total',con, schema=None, if_exists='replace', index=True, index_label=None, chunksize=None)


cursor5 = con.cursor()
sql5 = """SELECT Total.ItemInventoryAssemblyLnItemInventoryRefFullName, Total.TotalQTYUsed, InventoryPart.QuantityOnHand, (Total.TotalQTYUsed - InventoryPart.QuantityOnHand) AS QtyShorted
FROM Total INNER JOIN InventoryPart ON Total.ItemInventoryAssemblyLnItemInventoryRefFullName = InventoryPart.FullName
WHERE Total.TotalQTYUsed > InventoryPart.QuantityOnHand"""
cursor5.execute(sql5)
df5 = pd.DataFrame(cursor5.fetchall(),columns=['InventoryPart', 'TotalQTYUsed', 'QtyOnHand', 'QtyShorted'])
df5.to_sql('InventoryPart-Shorted',con,schema=None,if_exists='replace', index=True, index_label=None, chunksize=None)




cursor1.close()
cursor2.close()
cursor5.close()
con.close()






