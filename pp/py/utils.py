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
def ReadFile(filename):
      with open(filename) as f:
            filecontent=f.readlines()
      return ''.join(filter(lambda x:not x.startswith('#') or x.strip(),filecontent))

def log():
      logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='myapp.log',
                filemode='a+')
      return logging

def getnow():
      return time.strftime("%Y%m%d", time.localtime())

class hiveClient:
      def __init__(self,host,port):
            try:
                  self.transport = TSocket.TSocket(host, port)
                  self.transport = TTransport.TBufferedTransport(self.transport)
                  self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
                  self.client = ThriftHive.Client(self.protocol)
                  self.transport.open()
                  log().info('open hive client.............')
                  
            except Thrift.TException, tx:
                  log().error(tx.message)
                  raise
      def __enter__(self):
            return self
      
      def execute(self,hsql):
            self.client.execute(hsql)
            log().info(hsql)
            
      def fetchAll(self):
            return self.client.fetchAll()

      def fetchOne(self):
            return self.client.fetchOne()

      def __exit__(self,type,value,traceback):
            self.transport.close()
            return False

      
class mysql:
      def __init__(self,host,user,passwd,db):
            try:
                  self.conn= MySQLdb.connect(host=host,user=user,passwd=passwd,db=db)
                  log().info('open mysql conn......')
                  self.cursor=self.conn.cursor()
                       
            except Exception,e:
                  log().error(e)
      def __enter__(self):
            return self
      
      def execute(self,sql):
            return self.cursor.execute(sql)

      def commit(self):
            self.conn.commit()
      
      def __exit__(self,type,value,traceback):
            self.conn.close()
            return False
            
if __name__=='__main__':
      print ReadFile('splitCarM.txt')
      log().info('123')
