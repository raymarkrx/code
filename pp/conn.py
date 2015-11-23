#coding=utf-8
import sys
import os 
import cx_Oracle
import time
print time.time()
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
db=cx_Oracle.connect('chh','test','10.91.234.38/QASASIT1')
print db.version
dbc=db.cursor()
dbc.execute('select * from tm_stock_data_yahoo')
f=open('oracle.txt','a+')
while (1):  
    row = dbc.fetchone()
    if row == None:  
        break 
    list=[]
    for i in xrange(len(row)):
        list.append(str(row[i]))
    line= ','.join(list)
    f.write(line+'\n')
    f.flush()
f.close()
    
      
print "Number of rows returned: %d" % dbc.rowcount  
print time.time()
