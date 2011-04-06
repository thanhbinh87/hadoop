#! coding: utf-8
# pylint: disable-msg=W0311, E1101
import time
from tasks import fetch_logs
import datetime
import settings
#import urllib

while True:
  if int(time.time()) % 60 == 0:
    for ip in settings.SERVERS:
      print 'working...'
      delta = datetime.timedelta(minutes=2)
      t = datetime.datetime.now() - delta
      fetch_logs(ip, t)
  print 'sleeping...'
  time.sleep(1)
