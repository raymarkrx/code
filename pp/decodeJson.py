#!/usr/python
# -*- coding: utf-8 -*-
import json
import re
import sys
import logging
import time
import zipfile
import os
import commands
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%sS',
                filename='/home/dcplatform/python_use/myapp.log',
                filemode='a+')


# 映射关系FeatureCode=FeatureCode + FeatureCode
mapping={"BaseInfo":["LogFile","DataServiceLogFile","LogTimeStart"
"LogTimeEnd","UserName","Station","OS","AfsVersion", "VDIVersion","VDIConnect"],
"VehicleIdentify":["IdentifyType","Kilometre","VIN","FeatureCode"," Model","EngineModel","TransmissionModel"," Country","Platform","Brand","Year"],
"Functions":["Ecu","HW","Function","Channel","VCINumber","Flows","LogisticsRelated","LogisticsBefore","LogisticsAfter", "FunctionResult","FlowResult",
             "StartTime","EndTime"],
"LogisticsRelated":["Name","Value","DBValue"],
"LogisticsBefore":["Name","Value","DBValue"],
"LogisticsAfter":["Name","Value","DBValue"],
"Flows":["Flow","StartTime","EndTime","Timespan","FlowResult"]         
         }

####开始查找键值
global keys
keys=["BaseInfo","VehicleIdentify","Functions"]
###key in mapping
keyinmap=["LogisticsRelated","LogisticsBefore","LogisticsAfter","Flows"]

#json数据转换python数据结构
def decodejson(lns):
    
    try:
        de = json.loads(lns)
    except Exception,e:
        logging.error("%s " %(e))       
        logging.error("%s " %(lns))        
        return 
    for key,value in map(lambda x:(x,de[x]),keys):
        getfromkeyvalue(key,value)
        
#递归找出column值
def getfromkeyvalue(key,value):
    global ln
    for col in mapping.get(key):        
        if isinstance(value,dict)  :
            if isinstance(value.get(col),dict):                
                getfromkeyvalue(col, value.get(col,""))
            elif isinstance(value.get(col),list):
                if isinstance(value.get(col)[0],dict):
                    for i in xrange(len(value.get(col))):
                        getfromkeyvalue(col, value.get(col,"Null")[i])
                else:                    
                    ln=ln+"%s" %(','.join(value.get(col,"Null")) ,)+","
            else:                
                ln=ln+"%s" %(pasre(value.get(col,"Null"),"FeatureCode=\"(.+?)\" ","FamilyCode=\"(.+?)\" "),)+","                          
        elif isinstance(value,list) and len(value)>0:
            if isinstance(value[0],dict):
                for i in xrange(len(value)):
                    if isinstance (value[i].get(col,""),dict):
                        getfromkeyvalue(col,value[i].get(col,"Null"))
                    elif isinstance (value[i].get(col,""),list):
                        getfromkeyvalue(col,value[i].get(col,"Null"))
                    else:                        
                        ln=ln+"%s" %(value[i].get(col,"Null") ,)+","                        
            else:                
               ln=ln+"%s" %(','.join(value if value<>"None"or  value<>"" else "Null"),)+","
        else :
            if col in keyinmap:
                getfromkeyvalue(col, "")
            ln=ln+"Null," 
        ln=ln[:-1]+"\t"
        
#写入文件        
def jsonTotxt(fr,ft):
    fjson=open(fr,"r")
    ftxt=open(ft,"a+")    
    line=fjson.readline()
    logging.info("%s %s" %(fr,ft))
    global ln
    while len(line)<>0:        
        ln=""
        #if line:
            #decodejson(line)
        line and decodejson(line)
            #if ln:
                #ftxt.write(ln[:-1].encode("utf8")+"\n")
        ln and ftxt.write(ln[:-1].encode("utf8")+"\n")
        line=fjson.readline()
    ftxt.close()
    fjson.close()
    
#分解FeatureCode          
def pasre(st,pattern1,pattern2):
    if not st:
        return st
    if re.search(pattern1,st):
        p1=re.compile(pattern1)
        p2=re.compile(pattern2)
        return ",".join(p1.findall(st))+"\t"+",".join(p2.findall(st))
    else:
        return st
    
#解压缩文件
def unzipfiles1(filepath):
    for f in filter(lambda x:x.endswith('.gz'),os.listdir(filepath)):
        try:
            z=zipfile.ZipFile(filepath+"/"+f)
            for line in z.namelist():
                z.extract(line,'.')
                logging.info("unzip %s" %(line,))
        except Exception,e:
            logging.error("%s" %(e,))

def unzipfiles(filepath):
    for f in filter(lambda x:x.endswith('.gz'),os.listdir(filepath)):
        nf=filepath+'/'+f
        cmd="gzip -d  %s" %(nf,)
        (s,r)=commands.getstatusoutput(cmd);
        if s==0:
            logging.info("gzip successful %s " %(cmd))
        else:
            logging.info("gzip failed %s " %(cmd))
            
def downloadfromhadoop(path,local):
    cmd="/home/hdfs/hadoop-current/bin/hadoop  fs -get  %s %s" %(path,local)
    (s,r)=commands.getstatusoutput(cmd);
    if s==0:
        logging.info("download successful %s %s " %(cmd,r))
    else:
        logging.info("download failed %s  %s" %(cmd ,r))

def uploadtohadoop(src,det):
    cmd="/home/hdfs/hadoop-current/bin/hadoop  fs -put  %s  %s" %(src,det)
    (s,r)=commands.getstatusoutput(cmd);
    if s==0:
        logging.info("upload successful %s %s " %(cmd,r))
    else:
        logging.info("upload failed %s  %s" %(cmd ,r))
       
def checkfile(src):
    cmd="/home/hdfs/hadoop-current/bin/hadoop fs -ls  %s" %(src,)
    (s,r)=commands.getstatusoutput(cmd)
    if s==0:
        logging.info("found file %s %s %s " %(src,cmd,r))
        rmcmd ="/home/hdfs/hadoop-current/bin/hadoop fs -rm -skipTrash %slogfile*.txt" %(src,)
        (s,r)=commands.getstatusoutput(rmcmd)
        logging.info("%s %s " %(rmcmd,r))
    else:       
        logging.info("not found file %s %s  %s" %(src,cmd ,r))    
        mkcmd ="/home/hdfs/hadoop-current/bin/hadoop fs -mkdir  %s" %(src,)
        (s,r)=commands.getstatusoutput(mkcmd)
        logging.info("%s %s " %(mkcmd,r))
        
if __name__=="__main__":
    '''jsonfilename josn文件路径
      wirtetofilename json解析后的文件存放地址
      '''
    #"/home/logfile/20150722/*"
    dowownloadpath=sys.argv[3]+"/"+sys.argv[5]+"/*"
    #"/home/logfile_etl/20150722/"
    uploadpath=sys.argv[4]+"/"+sys.argv[5]+"/"
    pythonfilepath="/home/dcplatform/python_use"
    checkfile(uploadpath)
        #删除解压后的json文件
    for i in filter (lambda x:x.startswith('data'),os.listdir(pythonfilepath)):        
        os.remove(pythonfilepath+'/'+i) 
    downloadfromhadoop(dowownloadpath,pythonfilepath)
    #jsonfilename=pythonfilepath
    witetofilename=pythonfilepath+"/logfile.txt"
    unzipfiles(pythonfilepath)
    for i in filter (lambda x:x.startswith('data') ,os.listdir(pythonfilepath)):
        logging.info("process %s" %(i))
        jsonTotxt(pythonfilepath+'/'+i,witetofilename)
    #删除解压后的json文件
    for i in filter (lambda x:x.startswith('data'),os.listdir(pythonfilepath)):
        os.remove(pythonfilepath+'/'+i)
        logging.info("delete %s" %(i,))
    if os.path.exists(witetofilename):
        logging.info("exists %s upload to hdfs" %(witetofilename))
        uploadtohadoop(witetofilename,uploadpath)    
        os.remove(witetofilename)
        logging.info("remove   %s " %(witetofilename,))
