# -*- coding: utf-8 -*-

import urllib
import re
import time
import MySQLdb
import sys
print sys.stdin.encoding
reload(sys)
sys.setdefaultencoding('utf-8')  
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
dbconfig=db('10.91.227.145','jack','Jack1@34','test',3308,'utf8')     
@dbconfig
def execute(conn,sql,parm):   
    cursor =conn.cursor()
    data=cursor.executemany(sql,parm)
    if data:
        conn.commit()
        print sql
    print cursor.lastrowid
    
def getHtml(url):
    page = urllib.urlopen(url)
    time.sleep(0.05)
    html = page.read()
    return html


#<a href="http://q.10jqka.com.cn/stock/thshy/bsjd/">白色家电</a>
#<a href="(\w+)">(\w+)
def getstockname(url,bk):    
    stockpatten=re.compile(r'<td class="tc"><a href="http://stockpage.10jqka.com.cn/(\d+)/" target="_blank">(\S+)</a></td>')   
    m=stockpatten.findall(getHtml(url))
    parmlist=[]
    for i in  xrange(len(m)):
        #print bk,m[i][0]
        #execute('update tm_stock_code set hangye="%s" where stock_code="%s"'%(bk,m[i][0]))
        #print '股票代码：',m[i][0],'   股票名称：',m[i][1]
        parmlist.append((m[i][0],m[i][1],bk))
    parmlist= tuple(parmlist)
    print parmlist
    execute('insert into new_table(code,name,bank) values (%s,%s,%s) ',parmlist)

        
html = getHtml("http://q.10jqka.com.cn/stock/thshy/")        
p=re.compile(r'<a href="(?P<url>http://q.10jqka.com.cn/stock/thshy/\S+)">(?P<name>\S+)</a>')
m=  p.findall(html)
for i in  xrange(len(m)):
    #print '板块：',m[i][1]
    #print '********************************************************'
    getstockname(m[i][0],m[i][1])

#<td class="tc"><a href="http://stockpage.10jqka.com.cn/002503/" target="_blank">搜于特</a></td>
#<td class="tc"><a href="http://stockpage.10jqka.com.cn/002668/" target="_blank">
    #奥马电器</a></td>

