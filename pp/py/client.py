import sys
from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:
    transport = TSocket.TSocket('180.97.232.57', 10000)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    client = ThriftHive.Client(protocol)
    transport.open()
    hql = '''use aichuche_db'''
    print hql

    client.execute(hql)
    hql='select * from t_carstartstatusn_4'
    client.execute(hql)
    #client.execute("LOAD DATA LOCAL INPATH '/home/diver/data.txt' INTO TABLE people")
    #client.execute("SELECT * FROM people")
    #while (1):
    #  row = client.fetchOne()
    #  if (row == None):
    #    break
    #  print row
    #client.execute("SELECT count(*) FROM people")
    m= client.fetchAll()
    print type(m)

    transport.close()

except Thrift.TException, tx:
    print '%s' % (tx.message)
