# -*- coding: cp936 -*-
import MySQLdb
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import time
class mail163(object):
    def __init__(self,sender,receiver,subject,txt,username='raymarkrx',password = 'woaini123'):
        self.msg=MIMEText(txt,'html','utf8')
        self.msg['Subject'] = Header(subject, 'utf-8')
        self.msg['From'] = sender
        self.msg['To'] = receiver
        self.username=username
        self.password=password
        self.receiver=receiver
        self.sender=sender
    def send(self):
        smtp = smtplib.SMTP()
        smtp.connect('smtp.163.com')
        smtp.login(self.username, self.password)
        smtp.sendmail(self.sender, self.receiver, self.msg.as_string())
        smtp.quit()
        

class db(object):
    def __init__(self,host,user,passwd,db,port,charset):
        self.conn=MySQLdb.connect(host=host,user=user,
                                  passwd=passwd,db=db,port=port,charset=charset)

    def warp(self,func):
        print 'jjjj...'
        def _refunc(*args,**kwargs):
            print 'start warp................'
            f= func(self.conn,*args,**kwargs)
            self.conn.close()
            return f
        return _refunc


dbconfig=db('10.91.227.145','jack','Jack1@34','test',3308,'utf8')    

@dbconfig.warp
def query(conn,sql):
    
    cursor =conn.cursor()
    cursor.execute(sql)
    data= cursor.fetchall()
    return data

class Field(object):
    def __init__(self,name):
        self.name=name
        
class table(type):
    def __new__(cls,basename,parentname,attr):
        fields=[]
        tablename=''
        @dbconfig.warp
        def query(conn,self,sql):
            cursor =conn.cursor()
            cursor.execute(sql)
            data= cursor.fetchall()
            return data
        for i,j in attr.iteritems():
            if i=='__tablename__':
                tablename=j
            if isinstance(j,Field):
                fields.append(j.name)
        sql='select '+','.join(fields)+ ' from '+tablename
        attr['query']=lambda x=cls,y= sql:  query(x,y)
        return super(table,cls).__new__(cls,basename,parentname,attr)

class model(dict):
    __metaclass__=table
    
class email_config(model):
    __tablename__='tm_warn_email_config'
    pid=Field('pid')
    pdate=Field('pdate')
    depart_name=Field('depart_name')
    persion_name=Field('persion_name')
    email=Field('email')
    isused=Field('isused')
    orders=Field('orders')
    email_group=Field('email_group')
mm=email_config(id=2)


print query




#mail163('raymarkrx@163.com','raymarkrx@163.com',
        #'python email test','</pre><h1>%s</h1><pre>' %fs).send()

