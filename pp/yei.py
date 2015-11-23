#coding=utf8

import glob,os,time

modules = {}  
def load():
    for module_file in glob.glob('*-plugin.py'):        #glob.glob得到当前目录下匹配字符串的文件名       
            module_name, ext = os.path.splitext(os.path.basename(module_file))      #将文件名以点号分开  
            module = __import__(module_name)
            module.__filetime__=time.time()#获得模块  
            modules[module_name]=module

def relo():
    
print modules
