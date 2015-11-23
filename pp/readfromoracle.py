import sys
import os 
import cx_Oracle
import MySQLdb
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
#db=cx_Oracle.connect('chh','test','10.91.234.38/QASASIT1')
db=MySQLdb.connect(host='210.51.31.67',
        port = 3306,
        user='chh',
        passwd='test',
        db ='test',
)
sql="""select stock_data_date ,stock_code, shoupan_price,in_out from tm_stock_data_yahoo where stock_code='600637'
and stock_data_date between '2014-01-01' and '2014-12-31' order by stock_data_date  """
readsql=pd.read_sql(sql,con=db)
sql2="""select stock_code,begin_date,end_date from tm_stock_data_piece where stock_code='600637'"""
readpiece=pd.read_sql(sql2,con=db)
readpiece['begin_date']=readpiece['begin_date'].astype('datetime64')
readpiece['end_date']=readpiece['end_date'].astype('datetime64')

#readsql=readsql.cumsum()
readsql['stock_data_date']=readsql['stock_data_date'].astype('datetime64')
readsql['shoupan_price']=readsql['shoupan_price'].astype('float64')
#readsql['in_out']=readsql['in_out'].astype('float64')
readsql.index=readsql['stock_data_date']
readsql2=readsql.loc[:,['shoupan_price']]
readsql2.plot()
print readsql2.shoupan_price.idxmax()
print readsql2.xs(readsql2.shoupan_price.idxmax())
#print inout.values
for  i in xrange(len(readpiece.index)):
    
    a= readsql2.loc[readpiece.ix[i].begin_date:readpiece.ix[i].end_date]
    b=readsql2.iloc[i]
    print b
    x=a.index
    y=a['shoupan_price']
    plt.plot(x,y,color='r')
#s.plot(title='haoge')


#plt.scatter(x,y,color='r',alpha=1)

plt.show()
