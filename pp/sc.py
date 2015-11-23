#coding=utf-8
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import time
import os
    
def tick():
    print('Tick! The time is: %s' % datetime.now())
def tick2():
    print('Tick2! The time is: %s' % datetime.now())   
if __name__ == '__main__':
    scheduler = BlockingScheduler()
    #scheduler=BackgroundScheduler()
    scheduler.add_job(tick,'interval', seconds=3)
    scheduler.add_job(tick2,'interval', seconds=4)   
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown() 
