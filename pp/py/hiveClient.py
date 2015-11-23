import sys
from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
import time
import logging
import  MySQLdb
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='myapp.log',
                filemode='a+')
dt=time.strftime("%Y%m%d", time.localtime())
dt=sys.argv[1] if len(sys.argv)>1 else dt
try:
    transport = TSocket.TSocket('180.97.232.57', 10000)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    client = ThriftHive.Client(protocol)
    transport.open()
    with open('splitCarM.txt','r') as f:
          filecontent=f.readlines()
    
    filesql=(''.join(filecontent)).replace('$pt', dt)
    def runhive(sql):
          client.execute(sql)
          logging.debug(sql)
    map(lambda sql:runhive(sql),filter(lambda x:x.strip(),filesql.split(';')) )
    client.execute("select * from t_splitdistance2")
          
    #hql='select * from t_carstartstatusn_4'
    #client.execute(hql)
    #client.execute("LOAD DATA LOCAL INPATH '/home/diver/data.txt' INTO TABLE people")
    #client.execute("SELECT * FROM people")
    #while (1):
    #  row = client.fetchOne()
    #  if (row == None):
    #    break
    #  print row
    #client.execute("SELECT count(*) FROM people")
    m= client.fetchAll()
    conn=MySQLdb.connect(host='180.97.232.57',user='chh',passwd='test',db='aichuche_db')
    cursor=conn.cursor()
    #cursor.execute("delete from t_distance_classification where dt='"+str(dt)+"'")
    #conn.commit()
    for i in xrange(len(m)):
        sql="replace into  t_distance_classification (dt,device_id,serial_number,\
    start_dt,start_gpsx,start_gpsy,end_dt,end_gpsx,end_gpsy,create_time,update_time,remark)\
    values ('%s','%s',%s,'%s',%s,%s,'%s',%s,%s,now(),now(),'')" % tuple(m[i].split('\t'))
        cursor.execute(sql)
    conn.commit()
    conn.close()
    transport.close()

except Thrift.TException, tx:
    print '%s' % (tx.message)
    logging.info(tx.message)
