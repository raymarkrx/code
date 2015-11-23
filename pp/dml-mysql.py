import MySQLdb
import time
import json
class db(object):
    def __init__(self,host,user,passwd,db,port,charset):
        self.conn=MySQLdb.connect(host=host,user=user,
                                  passwd=passwd,db=db,port=port,charset=charset)
    def warp(self,func):
        
        def _refunc(*args,**kwargs):
            start=time.time()
            f= func(self.conn,*args,**kwargs)
            #self.conn.close()
            
            print '%s  %d'  % (args[0],time.time()-start)
            return f
        return _refunc
dbconfig=db('180.97.232.57','root','root','aichuche_db',3306,'utf8') 
@dbconfig.warp
def execute(conn,sql):    
    cursor =conn.cursor()
    data=cursor.execute(sql)
    
    return data

def insert():
    execute('insert into mysql_test values (null,now(),now())')

def update(id):
    execute('update mysql_test set update_time=now() where id=%d' %id)

for i in xrange(100):
    insert()
    update(i)
dbconfig.conn.close()
