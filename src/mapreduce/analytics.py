'''
Created on Mar 24, 2011

@author: rucney
'''

import time
import re
from urlparse import urlparse
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
 
  try:
    query = dict([part.split('=') for part in url[4].split('&')])
    res['referrer'] = query['referrer'] 
  except (ValueError, KeyError):
    res['referrer'] = None  
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

#def map(key, value):
#  try:
#    params = log_parsing(value)
#    status = params.get('status')
#    referrer = params.get('referrer')
#    
#    if status >= 200 and status < 300:
#      if referrer:
#        yield 'has_referrer', 1
#      else:
#        yield 'no_referrer', 1
#    else:
#      yield 'invalid', 1
#  except:
#    yield 'error', 1
##  pass
#
#def reduce(key, values):
#  yield key, sum(values)
  
#  pass
class FilterMapper:
    def __init__(self):
        self.start = int(self.params["start"])
        self.end = int(self.params["end"])
        
    def __call__(self, key, value):
      try:
        params = log_parsing(value)
        for i in params.get('duration'):
          
          if i >= self.start and i <= self.end:
            key = params.get('fid')
            value = params.get('avg_bytes_out') 
            yield key, value
      except:
        pass
def reducer(key,values):
  try:
    bytes_out = 0
    request_count = 0
    for value in values:
      bytes_out += int(value)
      request_count += 1
    value = "%s|%s" % (bytes_out, request_count)
    yield key,value
  except:
    pass
  
import heapq
# get top byte_out
def mapper2(key, value):
  try:
    value = "%s,%s" % (value.split('|')[0], key)
    yield 'bytes_out', value
  except:
    pass  
def reducer2(key, values):
  try:
    info = []
    for i in values:
      a = i.split(',')
      info.append({'bout': int(a[0]), 'fid': a[1]})    
    top_10 = heapq.nlargest(10, info, key=lambda k: k['bout'])
    for i in top_10:
      bytes_out = i['bout']
      fid = i['fid']
      yield fid, bytes_out
  except:
    pass

def map3(key, value):
  pass
def reduce3(key, values):
  pass

#def runner(job):
#  job.additer(FilterMapper, reducer, combiner=reducer)  
if __name__ == "__main__":
  
  import dumbo
  
  job = dumbo.Job()
  job.additer(FilterMapper, reducer)
  job.additer(mapper2, reducer2)
  job.run()


