#! coding: utf-8
# pylint: disable-msg=W0311
SERVERS = ['192.168.3.69',
           '192.168.6.126',
          '192.168.6.150',
          '192.168.6.159',
          '192.168.6.160',
          '192.168.6.188',
          '192.168.7.134'
          ]
#MONGODB_NODE = '192.168.6.147:20000'
#MONGODB_NODE_FID = '192.168.6.10:12345'
LOG_DIR = '/var/ramdisk/log/'
EXPIRE = 3600
DELTA = 1
hour = 60 * 60
day = 24 * hour
week = 7 * day
month = 30 * day
year = 12 * month
DELTA = [(1*hour,10), (1*day,60), (1*week,420), (1*month,1680), (1*year,20160)]
GROUP = ['baamboo', 'hosting', 'sannhac', 'bbmp3']

TEMP_DIR = '/tmp'
TIME_GR = 1800