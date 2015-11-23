from contextlib import contextmanager
import MySQLdb
class db(object):
    def __init__(self,host,user,passwd,db,port,charset):
        self.conn=MySQLdb.connect(host=host,user=user,
                                  passwd=passwd,db=db,port=port,charset=charset)
        self.pool=[self.conn]
        self.conn=None
    def __enter__(self):
      self.conn=self.pool.pop()
      print len(self.pool)
      return self.conn
    def __exit__(self,a,b,c):
      self.pool.append(self.conn)
      print len(self.pool)
        
@contextmanager
def make_context():
  print 'enter'
  yield db
  print 'exit'

with db('10.91.227.145','jack','Jack1@34','dlm',3308,'utf8')     as value :
  print value




          
  
