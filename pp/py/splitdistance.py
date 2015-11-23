import utils
import sys
rf=utils.ReadFile('splitCarM.txt')
pt=utils.getnow() if len(sys.argv)==1 else sys.argv[1]
rf=rf.replace('$pt',pt )
with utils.hiveClient('180.97.232.57', 10000) as hc:
      map(lambda sql:hc.execute(sql),rf.split(';'))
      hc.execute('select * from t_splitdistance2')
      m= hc.fetchAll()
      with utils.mysql(host='180.97.232.57',user='chh',passwd='test',db='aichuche_db') as my:
            for i in xrange(len(m)):
                  sql="replace into  t_distance_classification (dt,device_id,serial_number,\
    start_dt,start_gpsx,start_gpsy,end_dt,end_gpsx,end_gpsy,create_time,update_time,remark)\
    values ('%s','%s',%s,'%s',%s,%s,'%s',%s,%s,now(),now(),'')" % tuple(m[i].split('\t'))
                  my.execute(sql)
            my.commit()      
