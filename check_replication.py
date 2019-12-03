#!/usr/bin/env python
# -*- coding: utf-8 -*-
#-----------------------------
#--Create by: Omidreza Bagheri
#--Date: 2019-Nov-04
#----------------------------------------------------------------------------------
import MySQLdb as mysqldb
import sys
import datetime
#----------------------------------------------------------------------------------
logfile = "/var/log/check_replication.log"
#----------------------------------------------------------------------------------
masterHost = 'm1.m2.m3.m4'
masterUser = 'masterUser'
masterPass = 'masterPass'
slaveHost = 's1.s2.s3.s4'
slaveUser = 'slaveUser'
slavePass = 'slavePass'
#----------------------------------------------------------------------------------
debug = 'stdout'
#debug = 'stdlog'
#----------------------------------------------------------------------------------
def log(msg, logdate=True):
  cur = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
  if debug == "stdout":
    if logdate == True:
      print '"' + cur + '" "'  + msg + '"'
    else:
      print msg
  elif debug == "stdlog":
    lf = open(logfile, 'a')
    if logdate == True:
      lf.write('"' + str(cur) + '" "'  + msg + '"\n')
    else:
      lf.write(msg+'\n')
    lf.close()
#----------------------------------------------------------------------------------
def diff(mcur, scur, dbname, tbname):
  query = 'select count(*) from '+str(dbname)+'.'+str(tbname)
  try:
    mcur.execute(query)
    scur.execute(query)
  except:
    try:
      mcon = mysqldb.connect(masterHost, masterUser, masterPass)
      scon = mysqldb.connect(slaveHost, slaveUser, slavePass)
      mcur = mcon.cursor()
      scur = scon.cursor()
      mcur.execute(query)
      scur.execute(query)
    except Exception, e:
      print str(e)
      exit(0)
  mrow = mcur.fetchone()
  srow = scur.fetchone()
  if mrow[0] != srow[0]:
    diff = mrow[0]-srow[0]
    log('master: '+dbname+'.'+tbname+' '+str(mrow[0]),False)
    log('slave : '+dbname +'.'+tbname+' '+str(srow[0])+' (diff: '+str(diff)+')',False)
#----------------------------------------------------------------------------------
if len(sys.argv) != 3 and len(sys.argv) != 2:
  print 'Usage: python check_replication DB_NAME TABLE_NAME'
  exit(0)

mcon = mysqldb.connect(masterHost, masterUser, masterPass)
scon = mysqldb.connect(slaveHost, slaveUser, slavePass)
mcur = mcon.cursor()
scur = scon.cursor()

if len(sys.argv) == 3:
  dbname = sys.argv[1]
  tbname = sys.argv[2]
  log("Starting the program 'check_replication' on '"+str(dbname)+"'.'"+str(tbname)+"'")
  diff(mcur, scur, dbname, tbname)

elif len(sys.argv) == 2:
  dbname = sys.argv[1]
  log("Starting the program 'check_replication' on '"+str(dbname)+"'.*")
  query = "select TABLE_NAME from information_schema.TABLES where table_schema = '"+str(dbname)+"' and table_name != 'AUDT_UserAccessPages' and table_name != 'SysAudit' and TABLE_TYPE = 'BASE TABLE'"
  scur.execute(query)
  rows = scur.fetchall()
  for tbname in rows:
    tbname = tbname[0]
    tbname = diff(mcur, scur, dbname, tbname)
