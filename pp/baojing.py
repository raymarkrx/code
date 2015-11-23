#coding: utf-8
import smtplib
from email.mime.text import MIMEText
from email.header import Header

sender = 'raymarkrx@163.com'
receiver = 'longredhao@163.com'
subject = 'python email test'
smtpserver = 'smtp.163.com'
username = 'raymarkrx'
password = 'woaini123'

msg = MIMEText('</pre><h1>你好</h1><pre>','html','utf-8') 
msg['Subject'] = Header(subject, 'utf-8')
msg['From'] = sender
msg['To'] = receiver
smtp = smtplib.SMTP()
smtp.connect('smtp.163.com')
smtp.login(username, password)
smtp.sendmail(sender, receiver, msg.as_string())
smtp.quit()
