#coding=utf8
import tornado.ioloop
import tornado.web
import time
import MySQLdb
import json
import tornado.gen

class db(object):
    def __init__(self,host,user,passwd,db,port,charset):
        self.host=host
        self.user=user
        self.passwd=passwd
        self.db=db
        self.port=port
        self.charset=charset
        self._createConn()

    def _createConn(self):
        self.conn=MySQLdb.connect(host=self.host,user=self.user,
                                  passwd=self.passwd,db=self.db,port=self.port,charset=self.charset)

    def __call__(self,func):     
        def _refunc(*args,**kwargs):
            try:
                self.conn.ping()
                
            except:
                self._createConn()
                
            f= func(self.conn,*args,**kwargs)
            return f
        return _refunc
class Row(dict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

dbconfig=db('10.91.227.145','jack','Jack1@34','dlm',3308,'utf8')    
#dbconfig=db('localhost','root','root','aichuche_db',3307,'utf8')    
@dbconfig
def execute(conn,sql):   
    cursor =conn.cursor()
    cursor.execute(sql)
    data= cursor.fetchall()
    column_names = [d[0] for d in cursor.description]
    conn.commit()
    return data,column_names

def html(data,column_names):
    headstring='<!meta http-equiv="refresh" content="5">'
    trstring=''
    for i in xrange(len(column_names)):
        headstring=headstring+'<td >%s</td>' %column_names[i]
        trstring=trstring+'<td >%s</td>'
    trstring='<tr>'+trstring+'</tr>'
    bodystring='<table border="1" ><tr bgcolor="red" align="center">'+headstring
    for i in xrange(len(data)):
          bodystring=bodystring+ trstring % data[i]   +'\n'
    return bodystring+'</table>'



class MainHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self,tablename):
        try: 
            data,column= execute('select * from %s  limit 20' % tablename)
            if 'id'  in column:
                data,column=execute('select * from %s  order by id desc   limit 20' % tablename)
            htmlstring=html(data,column)
        except:
            return self.write('<h1>Table Error</h1>')
        output=htmlstring.encode('utf8')
        self.write(output)


class MainHandler2(tornado.web.RequestHandler):
    def get(self):
        try:
            data,column= execute('show tables')
            print data,column
            #<a href="http://www.nowamagic.net/">首页</a>
            data=[ "<a href='http://localhost:8000/query/"+data[i][0]+"'>"+data[i][0]+'</a>'  for i in xrange(len(data))]
            print data
            htmlstring=html(data,column)
        except Exception as e :
            print e
            return self.write('<h1>Table Error OR Table No Id</h1>')
        output=htmlstring.encode('utf8')
        self.write(output)
application = tornado.web.Application([
    (r"/query/(\w+)", MainHandler),(r"/", MainHandler2)
])

if __name__ == "__main__":
    application.listen(8000)
    tornado.ioloop.IOLoop.current().start()
