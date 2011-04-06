'''
Created on Apr 6, 2011

@author: rucney
'''
import heapq
values = ['1,001', '4,004', '9,09', '5,005']
info = []
l = []
for i in values:
  a = i.split(',')
  info.append({'bout': int(a[0]), 'fid': a[1]})    
top_10 = heapq.nlargest(10, info, key=lambda k: k['bout'])
for i in top_10:
  bytes_out = i['bout']
  fid = i['fid']
  l.append(bytes_out)
print l