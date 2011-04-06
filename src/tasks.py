#! coding: utf-8
# pylint: disable-msg=W0311
#import api
#from celery.decorators import task
import urllib
import time
import re
from urlparse import urlparse
import os
import settings
def log_parsing(line=''):
  """ parse log :{time:bytes_out:group...}"""
  parts = [
      r'(?P<remote_host>\S+)', # host %h
      r'\[(?P<time>.+)\]', # time %t
      r'"(?P<request>.+)"', # request "%r"
      r'(?P<status>[0-9]+)', # status %>s
      r'(?P<size>\S+)', # size %b (careful, can be '-')
      r'"(?P<referer>.*)"', # referer "%{Referer}i"
      r'"(?P<agent>.*)"', # user agent "%{User-agent}i"
      r'(?P<bytes_in>\S+)', # %I   
      r'(?P<bytes_out>\S+)', # %O   
      r'(?P<time_used>\S+)', # %T  
      r'(?P<domain>\S+)' # %V
  ]
  pattern = re.compile(r'\s+'.join(parts) + r'\s*\Z')
  m = pattern.match(line)
  if not m:
    return {}
  res = m.groupdict()
  res["status"] = int(res["status"])
  url = urlparse(res['request'])
  
  res["fid"] = url[2].split('/')[-1].split('.')[0]
  try:
    query = dict([part.split('=') for part in url[4].split('&')])
    res['group'] = query['group'] 
  except (ValueError, KeyError):
    res['group'] = 'unknown'
 
      
  res["time_used"] = int(res["time_used"])
  
  if res["size"] == "-":
    res["size"] = 0
  else:
    res["size"] = int(res["size"])
    
  if res["bytes_out"] == "-":
    res["bytes_out"] = 0
  else:
    res["bytes_out"] = int(res["bytes_out"])
  tt = time.strptime(res["time"][:-6], "%d/%b/%Y:%H:%M:%S")
  res["time"] = int(time.mktime(tt))
  
  res['time']
  time_used = res['time_used']
  res['duration'] = range(res['time'] + 1 - time_used, res['time'] + 1)
  if time_used == 0:
    bytes_out = res['bytes_out']
  else:
    bytes_out = res['bytes_out'] / time_used
  res['avg_bytes_out'] = bytes_out
#  prin
  return res
#@task()
def fetch_logs(ip, t):  
  try:
    log_file = "%s/%02d/%02d/%02d/%02d" % (t.year, t.month, t.day, t.hour, t.minute)
    print log_file
    url = 'http://%s:2309/fetch/%s' % (ip, log_file)
    print url
    data = urllib.urlopen(url).read()
    
    filename = os.path.join(log_file, '%s.log' % ip)
    filename = filename.replace('/', '_')
    temp_file = os.path.join(settings.TEMP_DIR, filename)
    
    print temp_file
    open(temp_file, 'w').write(data)
    command = '''hadoop-0.20 fs -put %s /user/log/%s;rm -f %s''' % (temp_file, filename, temp_file)
    
    os.popen(command)
  except:
    print 'error'
#  if data != '':
#    lines = data.split("\n")
#    for line in lines:
#      if line and line != '':
#        try:
#          params = log_parsing(line)
#          api.log(params)
#        except:
#          print 'asjfhsaj'
#          print line
        
  return True
#if __name__ == '__main__':
#  line = '''113.170.178.176 [24/Mar/2011:17:15:03 +0700] "GET /508cc327a2a0c0663ab33eee6e7f520e/4d8b1268/dev5/0/000/131/0000131902.flv?group=baamboo&referrer=unknown&filetype=flv&start=0 HTTP/1.1" 206 176766 "http://nf1.vcmedia.vn/508cc327a2a0c0663ab33eee6e7f520e/4d8b1268/dev5/0/000/131/" "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.1.2) Gecko/20090729 Firefox/3.5.2" 398 177144 1036 nf1.vcmedia.vn
#'''
#  line = '''183.81.62.32 [24/Mar/2011:17:17:19 +0700] "GET /022ff0428d17bc16899a90e8ab416820/4d8b19a1/dev18/0/000/054/0000054141.flv?group=hosting&referrer=static.hosting.vcmedia.vn&filetype=flv HTTP/1.1" 200 28068177 "http://static.hosting.vcmedia.vn/players/player.swf?key=2fa0014b886646b5b86ec37580fe3c69&pname=kenh14player.swf" "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; InfoPath.2; AskTbAF2/5.9.0.12758)" 528 28068421 180 nh2.vcmedia.vn
#'''
#  a = log_parsing(line)
#  api.log(a)
