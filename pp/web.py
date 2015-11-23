#coding=utf8
import time
from wsgiref.simple_server import make_server
import MySQLdb
import urllib
import json
def getHtml(url):
    page = urllib.urlopen(url)
    html = page.read()
    return html

class db(object):
    def __init__(self,host,user,passwd,db,port,charset):
        self.conn=MySQLdb.connect(host=host,user=user,
                                  passwd=passwd,db=db,port=port,charset=charset)

    def warp(self,func):     
        def _refunc(*args,**kwargs):
            print time.time()
            f= func(self.conn,*args,**kwargs)
            print time.time()
            return f
        return _refunc


dbconfig=db('10.91.227.145','jack','Jack1@34','dlm',3308,'utf8')    

@dbconfig.warp
def execute(conn,sql):   
    cursor =conn.cursor()
    cursor.execute(sql)
    data= cursor.fetchall()
    datas='<table border="1" align="center"><tr bgcolor="red" align="center"><td >ID</td><td>USERNAME</td><td>NAME</td><td>ORG</td><td>TYPE</td></tr>'
    #for i in xrange(len(data)):
          #datas=datas+ '<tr><td>%d</td><td>%s</td><td>%s</td><td>%d</td><td>%d</td></tr>' % data[i]   +'\n'
    #return datas+'</table>'
    data=[dict(zip(('id','username','name','org','type'), data[i]))for i in xrange(len(data))]
    return json.dumps(data)






class Router(object):
    def __init__(self):
        self.path_info = {}
    def route(self, environ, start_response):
        if environ['PATH_INFO'] in self.path_info.iterkeys():
            application = self.path_info[environ['PATH_INFO']]
            return application(environ, start_response)
        else:
            return hello_world_app(environ, start_response)
    def __call__(self, path):
        def wrapper(application):
            self.path_info[path] = application
        return wrapper
router = Router()

#here is the application
@router('/hello')
def hello(environ, start_response):
    status = '200 OK'
    output = '127.0.0.1 - - [27/Oct/2015 13:55:49] "GET /hello HTTP/1.1" 200 122'
    response_headers = [('Content-type', 'text/html')]
    parm=environ['QUERY_STRING'].split('&')
    time.sleep(10)
    write = start_response(status, response_headers)
    data=execute('select  user_id,user_name,name,org_id,type from t_user limit 1')
    output=data.encode('utf8')
    print type(output)
    return output


@router('/world')
def world(environ, start_response):
    status = '200 OK'
    output ='<table><tr><td>10000000000000</td><td>21B186131zjl</td><td>李士忠</td><td>210000000000062</td><td>2</td></tr></table>'
    print type(output)
    response_headers = [('Content-type', 'text/html')]
    write = start_response(status, response_headers)
    return output
#here run the application
result = router.route

    
def hello_world_app(environ, start_response):
    status = '200 OK' # HTTP Status
    headers = [('Content-type', 'text/plain')] # HTTP Headers
    start_response(status, headers)
    print type(environ)
    print environ
    
    # The returned object is going to be printed
    return environ

httpd = make_server('', 8000, router.route)
print "Serving on port 8000..."

# Serve until process is killed
httpd.serve_forever()
