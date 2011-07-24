#!/usr/bin/python
import os
import sys
import time
import cx_Oracle

### Configuration Area
main_db_username = 'sys'
main_db_password = ''
main_db_hostname = ''
main_db_name = ''
main_db_domain = ''
main_db_unique_name = main_db_name
main_db_service = main_db_unique_name + '.' + main_db_domain
main_oracle_sid = ''
main_oracle_home = '/u01/base/db'
main_grid_home = '/u01/grid'
main_grid_sid = '+ASM'
main_oracle_base = '/u01/base'
main_dg_data_name = 'DATA'
main_dg_fra_name = 'FRA'

backup_db_username = 'sys'
backup_db_password = ''
backup_db_global_hostname = ''
backup_db_private_hostname = ''
backup_db_name = main_db_name
backup_db_domain = ''
backup_db_unique_name = ''
backup_db_service = backup_db_unique_name + '.' + backup_db_domain
backup_oracle_sid = ''
backup_oracle_home = '/u01/base/db'
backup_grid_home = '/u01/grid'
backup_grid_sid = '+ASM'
backup_oracle_base = '/u01/base'
backup_dg_data_name = 'DATA'
backup_dg_fra_name = 'FRA'

compatible = '11.2.0.2'

#####  Do not edit below unless you understand what you're doing  #####


def get_yes_or_no(): 
    for line in iter(sys.stdin.readline, ""): 
        if (line == "y\n"): 
            return("yes") 
        elif (line == "n\n"): 
            return("no") 
        else: 
            print "Enter just 'y' or 'n'. [y/n]: ",

###
# generate listener.ora for Primary Database
###
str = """
SID_LIST_LISTENER =
(SID_LIST =
    (SID_DESC =
        (GLOBAL_DBNAME = %s)
        (ORACLE_HOME = %s)
        (SID_NAME = %s)
    )
)
""" % (main_db_service, main_oracle_home, main_oracle_sid)
print "Please add following entry to %s/network/admin/listener.ora on Primary Database and Restart listener." % main_grid_home
print str
print "When finish editting, press [y]:",

try:
    res = get_yes_or_no()
except:
    sys.exit()

if not res == "yes":
    sys.exit()
print ""

con_main = cx_Oracle.connect(main_db_username, main_db_password, main_db_hostname + ':/' + main_db_service, cx_Oracle.SYSDBA)
cur_main = con_main.cursor()

##
# enable archive log mode on main.
##
#sql = "select log_mode from v$database"
#cur_main.execute(sql)
#res = cur_main.fetchone()
#if not res[0] == "ARCHIVELOG":
#    ## confirm
#    "Archive Log Mode is required to be set on Primary Database but not enabled at present. Enable it now? (Primary Database is going to be shutdown) [y/n]:",
#    res = get_yes_or_no()
#    if res == "no":
#        sys.exit()
#    print ""
#
#    ## enable archive log
#    print "Enabling archive log mode on Primary Database..."
#    sqls = []
#    sqls.append("startup mount force")
#    sqls.append("alter database archive log")
#    sqls.append("alter database open")
#    for sql in sqls:
#        try:
#            cur_main.execute(sql)
#        except:
#            print "Failed: %s" % sql
#            sys.exit()
#    print "Done.\n"


##
# enable force logging on main.
##
sql = "select force_logging from v$database"
cur_main.execute(sql)
res = cur_main.fetchone()
if not res[0] == "YES":
    print "Enabling force logging on Primary Database..."
    sql = "alter database force logging"
    try:
        cur_main.execute(sql)
    except cx_Oracle.DatabaseError,msg:
        print "Failed: sql=%s" % sql, msg
        sys.exit()
    print "Done.\n"


##
# configure init parameters on main.
##
print "Configuring initialization parameters on Primary Database..."
sqls = []
sqls.append("alter system set db_unique_name='%s' scope=spfile" % main_db_unique_name)
sqls.append("alter system set log_archive_config='dg_config=(%s,%s)' scope=both" % (main_db_unique_name, backup_db_unique_name))
sqls.append("alter system set log_archive_dest_1='location=+%s VALID_FOR=(all_logfiles,all_roles)' scope=both" % main_dg_fra_name)
sqls.append("alter system set log_archive_dest_2='service=\"%s\" SYNC VALID_FOR=(online_logfile,primary_role) db_unique_name=\"%s\"' scope=both" % (backup_db_unique_name, backup_db_unique_name))
sqls.append("alter system set log_archive_dest_state_1=enable scope=both")
sqls.append("alter system set log_archive_dest_state_2=enable scope=both")
sqls.append("alter system set log_archive_max_processes=30 scope=both")
sqls.append("alter system set fal_server=\"%s\" scope=both" % backup_db_unique_name)
sqls.append("alter system set standby_file_management=auto scope=both")
for sql in sqls:
    try:
        cur_main.execute(sql)
    except cx_Oracle.DatabaseError,msg:
        print "Failed: sql=%s" % sql, msg
        sys.exit()
# restart instance to validate the configuration
cur_main.close()
print "Restarting Primary Database..."
print "Shutting down..."
try:
    con_main.shutdown(cx_Oracle.DBSHUTDOWN_IMMEDIATE)
except cx_Oracle.DatabaseError,msg:
    print "Failed.", msg
    sys.exit()
cur_main = con_main.cursor()
print "Closing..."
sqls = ["alter database close normal","alter database dismount"]
for sql in sqls:
    try:
        cur_main.execute(sql)
    except cx_Oracle.DatabaseError,msg:
        print "Failed: sql=%s" % sql, msg
        sys.exit()
print "Completing shutdown..."    
try:
    con_main.shutdown(mode = cx_Oracle.DBSHUTDOWN_FINAL)
except cx_Oracle.DatabaseError,msg:
    print "Failed.", msg
    sys.exit()

print "Connecting with PRELIM_AUTH mode..."
try:
    con_main = cx_Oracle.connect(main_db_username, main_db_password, main_db_hostname + ':/' + main_db_service, cx_Oracle.SYSDBA | cx_Oracle.PRELIM_AUTH)
except cx_Oracle.DatabaseError,msg:
    print "Failed.", msg
    sys.exit()

print "Starting as nomount..."
try:
    con_main.startup()
except cx_Oracle.DatabaseError,msg:
    print "Failed.", msg
    sys.exit()

print "Connecting..."
try:
    con_main = cx_Oracle.connect(main_db_username, main_db_password, main_db_hostname + ':/' + main_db_service, cx_Oracle.SYSDBA)
except cx_Oracle.DatabaseError,msg:
    print "Failed.", msg
    sys.exit()

cur_main = con_main.cursor()

print "Transitioning to mount..."
try:
    sql = "alter database mount"
    cur_main.execute(sql)
except cx_Oracle.DatabaseError,msg:
    print "Failed: sql=%s" % sql, msg
    sys.exit()

print "Transitioning to open..."
try:
    sql = "alter database open"
    cur_main.execute(sql)
except cx_Oracle.DatabaseError,msg:
    print "Failed: sql=%s" % sql, msg
    sys.exit()

print "Done.\n"


##
# create asm directory for spfile on backup.
##
print "Creating ASM Directory for control files..."
res = os.system("ORACLE_HOME=%s ORACLE_SID=%s %s/bin/asmcmd ls +%s/%s > /dev/null 2>&1" % (backup_grid_home, backup_grid_sid, backup_grid_home, backup_dg_data_name, backup_db_unique_name))
if not res == 0:
    cmd = "ORACLE_HOME=%s ORACLE_SID=%s %s/bin/asmcmd mkdir +%s/%s > /dev/null 2>&1" % (backup_grid_home, backup_grid_sid, backup_grid_home, backup_dg_data_name, backup_db_unique_name)
    res = os.system(cmd)
    if not res == 0:
        print "Failed: %s" % cmd
        sys.exit()
print "Done.\n"


##
# create init parameter on backup.
##
print "Creating spfile for Standby Database..."
str = """
%(backup_oracle_sid)s.__db_cache_size=297795584
%(backup_oracle_sid)s.__java_pool_size=4194304
%(backup_oracle_sid)s.__large_pool_size=12582912
%(backup_oracle_sid)s.__pga_aggregate_target=343932928
%(backup_oracle_sid)s.__sga_target=515899392
%(backup_oracle_sid)s.__shared_io_pool_size=0
%(backup_oracle_sid)s.__shared_pool_size=184549376
%(backup_oracle_sid)s.__streams_pool_size=4194304
*.audit_file_dest='%(backup_oracle_base)s/admin/%(backup_db_unique_name)s/adump'
*.audit_trail='db'
*.compatible='%(compatible)s'
*.db_block_size=8192
*.db_create_file_dest='+%(backup_dg_data_name)s'
*.db_domain='%(backup_db_domain)s'
*.db_name='%(backup_db_name)s'
*.db_unique_name='%(backup_db_unique_name)s'
*.db_recovery_file_dest='+%(backup_dg_fra_name)s'
*.db_recovery_file_dest_size=4227858432
*.diagnostic_dest='%(backup_oracle_base)s'
*.dispatchers='(PROTOCOL=TCP) (SERVICE=%(main_oracle_sid)sXDB)'
*.fal_server='%(main_db_unique_name)s'
*.log_archive_config='dg_config=(%(main_db_unique_name)s,%(backup_db_unique_name)s)'
*.log_archive_dest_1='location=+%(backup_dg_fra_name)s VALID_FOR=(all_logfiles,all_roles)'
*.log_archive_dest_2='service=%(main_db_unique_name)s SYNC VALID_FOR=(online_logfile,primary_role) db_unique_name=%(main_db_unique_name)s'
*.log_archive_dest_state_1='ENABLE'
*.log_archive_dest_state_2='ENABLE'
*.log_archive_format='%%t_%%s_%%r.dbf'
*.log_archive_max_processes=30
*.memory_target=858783744
*.open_cursors=300
*.processes=150
*.remote_login_passwordfile='EXCLUSIVE'
*.standby_file_management='AUTO'
*.undo_tablespace='UNDOTBS1'
""" % {"main_db_unique_name":main_db_unique_name,"main_db_service":main_db_service,"backup_db_name":backup_db_name,"backup_db_unique_name":backup_db_unique_name,"main_dg_data_name":main_dg_data_name,"backup_dg_data_name":backup_dg_data_name,"main_dg_fra_name":main_dg_fra_name,"backup_dg_fra_name":backup_dg_fra_name,"main_oracle_sid":main_oracle_sid,"backup_oracle_sid":backup_oracle_sid,"backup_oracle_base":backup_oracle_base,"backup_db_domain":backup_db_domain,"compatible":compatible}
f = open("/tmp/pfile", "w")
f.write(str)
f.close()
# create spfile
cmd = """ORACLE_HOME=%s ORACLE_SID=%s %s/bin/sqlplus %s/%s as sysdba > /dev/null 2>&1 <<EOF
create spfile='+%s/%s/spfile%s.ora' from pfile='/tmp/pfile';
EOF""" % (backup_oracle_home, backup_oracle_sid, backup_oracle_home, backup_db_username, backup_db_password, backup_dg_data_name, backup_db_unique_name, backup_oracle_sid)
res = os.system(cmd)
if not res == 0:
    print "Failed: %s" % cmd
    sys.exit()
#os.remove("/tmp/pfile")
# create init[SID].ora
str = "spfile='+%s/%s/spfile%s.ora'" % (backup_dg_data_name, backup_db_unique_name, backup_oracle_sid)
try:
    f = open("%s/dbs/init%s.ora" % (backup_oracle_home, backup_oracle_sid), "w")
    f.write(str)
    f.close()
except:
    print "Failed to create init%s.ora." % backup_oracle_sid
    sys.exit()

print "Done.\n"



##
# create tnsnames.ora on main and backup.
##
print "Creating tnsnames.ora..."
str = """
%s =
(DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = 1521))
    (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = %s))
)
%s =
(DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = 1521))
    (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = %s))
)
%s_tmp =
(DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = 1521))
    (CONNECT_DATA = (SERVER = DEDICATED)(SID = %s))
)
%s_tmp =
(DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = 1521))
    (CONNECT_DATA = (SERVER = DEDICATED)(SID = %s))
)
""" % (main_db_unique_name, main_db_hostname, main_db_service, backup_db_unique_name, backup_db_global_hostname, backup_db_service, main_db_unique_name, main_db_hostname, main_oracle_sid, backup_db_unique_name, backup_db_global_hostname, backup_oracle_sid)
try:
    f = open("%s/network/admin/tnsnames.ora" % backup_oracle_home, "w")
    f.write(str)
    f.close()
except:
    print "Failed to create %s/network/admin/tnsnames.ora" % backup_oracle_home
    sys.exit()
print "Done.\n"

print "Please copy %s/network/admin/tnsnames.ora on Standby Database to the same location of Primary Database. When copy finished, plese enter [y]:" % (backup_oracle_home),
try:
    res = get_yes_or_no()
except:
    sys.exit()

if res == "no":
    sys.exit()
print ""


##
# create password file on backup.
##
print "Creating password file on Standby Database..."
res = os.system("ls %s/dbs/orapw%s > /dev/null 2>&1" % (backup_oracle_home, backup_oracle_sid))
if not res == 0:
    cmd = "%s/bin/orapwd file=%s/dbs/orapw%s password=%s" % (backup_oracle_home, backup_oracle_home, backup_oracle_sid, backup_db_password)
    res = os.system(cmd)
    if not res == 0:
        print "Failed: %s" % cmd
        sys.exit()
print "Done.\n"


##
# create listener.ora on backup.
##
# create file
print "Creating listener.ora on Standby Database and restart listener..."
str = """
SID_LIST_LISTENER =
(SID_LIST =
    (SID_DESC =
        (GLOBAL_DBNAME = %s)
        (ORACLE_HOME = %s)
        (SID_NAME = %s)
    )
)
LISTENER =
  (DESCRIPTION_LIST =
    (DESCRIPTION =
      (ADDRESS = (PROTOCOL = IPC)(KEY = EXTPROC1521))
      (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = 1521))
    )
  )
ADR_BASE_LISTENER = %s
ENABLE_GLOBAL_DYNAMIC_ENDPOINT_LISTENER=ON
""" % (backup_db_service, backup_oracle_home, backup_oracle_sid, backup_db_private_hostname, backup_oracle_base)
try:
    f = open("%s/network/admin/listener.ora" % backup_grid_home, "w")
    f.write(str)
    f.close()
except:
    print "Failed to create %s/network/admin/listener.ora" % backup_grid_home
    sys.exit()

# restart listener on backup.
cmd = "%s/bin/srvctl stop listener -l listener > /dev/null 2>&1" % (backup_grid_home)
res = os.system(cmd)
if not res == 0:
    print "Failed: %s" % cmd
    sys.exit()

cmd = "%s/bin/srvctl start listener -l listener > /dev/null 2>&1" % (backup_grid_home)
res = os.system(cmd)
if not res == 0:
    print "Failed: %s" % cmd
    sys.exit()
print "Done.\n"


##
# create adump directory for audit on backup.
##
print "Creating adump directory on Standby Database..."
res = os.system("ls %s/admin/%s/adump > /dev/null 2>&1" % (backup_oracle_base, backup_db_unique_name))
if not res == 0:
    cmd = "mkdir -p %s/admin/%s/adump > /dev/null 2>&1" % (backup_oracle_base, backup_db_unique_name)
    res = os.system(cmd)
    if not res == 0:
        print "Failed: %s" % cmd
        sys.exit()
print "Done.\n"


##
# launch db instance as nomount on backup.
##
print "Launching Database Instance as nomount on Standby Database..."
cmd = """ORACLE_HOME=%s ORACLE_SID=%s %s/bin/sqlplus %s/%s as sysdba > /dev/null 2>&1 <<EOF
startup nomount force;
EOF""" % (backup_oracle_home, backup_oracle_sid, backup_oracle_home, backup_db_username, backup_db_password)
res = os.system(cmd)
if not res == 0:
    print "Failed: %s" % cmd
    sys.exit()
print "Done.\n"



## 
# import db from main to backup using rman. 
##
print "Going to import databaase from %s to %s. Are you sure? [y/n]:" % (main_db_unique_name, backup_db_unique_name),
try:
    res = get_yes_or_no()
except:
    sys.exit()

if res == "no":
    sys.exit()
print ""

cmd = """ORACLE_HOME=%s ORACLE_SID=%s %s/bin/rman target %s/%s@%s_tmp auxiliary %s/%s@%s_tmp <<EOF
duplicate target database for standby from active database dorecover;
EOF""" % (backup_oracle_home, backup_oracle_sid, backup_oracle_home, main_db_username, main_db_password, main_db_unique_name, backup_db_username, backup_db_password, backup_db_unique_name)
res = os.system(cmd)
if not res == 0:
    print "Failed: %s" % cmd
    sys.exit()
print "Done.\n"


print "Establishing connection to Standby Database..."
try:
    con_backup = cx_Oracle.connect(backup_db_username, backup_db_password, backup_db_private_hostname + ':/' + backup_db_service, cx_Oracle.SYSDBA)
    cur_backup = con_backup.cursor()
except:
    print "Failed to connect to Standby Database."
    sys.exit()
print "Done.\n"

print "Transitioning to open..."
try:
    sql = "alter database open"
    cur_backup.execute(sql)
except:
    print "Failed: sql=%s" % sql, msg
    sys.exit()
print "Done.\n"


##
# create standby redo log on backup.
##
# get max number of redo log group
print "Creating standby redo log on Standby Database..."
try:
    sql = "select max(group#) from v$log"
    cur_main.execute(sql)
except cx_Oracle.DatabaseError,msg:
    print "Failed: sql=%s" % sql, msg
    sys.exit()
res = cur_main.fetchone()
max_log_group_num = res[0]
# get online redo log group info on main
try:
    sql = "select bytes from v$log"
    cur_main.execute(sql)
except cx_Oracle.DatabaseError,msg:
    print "Failed: sql=%s" % sql, msg
    sys.exit()
rows = cur_main.fetchmany()
log_group_num = max_log_group_num + 1
for row in rows:
    try:
        sql = "alter database add standby logfile group %s size %s" % (log_group_num, row[0])
        cur_backup.execute(sql)
    except cx_Oracle.DatabaseError,msg:
        print "Failed: sql=%s" % sql, msg
        sys.exit()
    log_group_num = log_group_num + 1
# Since one more standby redo log group should be created, create it.
try:
    sql = "alter database add standby logfile group %s size %s" % (log_group_num, row[0])
    cur_backup.execute(sql)
except cx_Oracle.DatabaseError,msg:
    print "Failed: sql=%s" % sql, msg
    sys.exit()
print "Done.\n"




##
# start replication from main to backup.
##
# confirm
print "Starting replication from %s to %s. Are you sure? [y/n]:" % (main_db_unique_name, backup_db_unique_name),
try:
    res = get_yes_or_no()
except:
    sys.exit()

if res == "no":
    sys.exit()
print ""

# start replication
try:
    sql = "alter database recover managed standby database using current logfile disconnect from session"
    cur_backup.execute(sql)
except cx_Oracle.DatabaseError,msg:
    print "Failed: sql=%s" % sql, msg
    sys.exit()
print "Done.\n"

# transition to Maximum Availability mode
print "Transitioning to Maximum Availability mode..."
try:
    sql = "alter database set standby database to maximize availability"
    cur_main.execute(sql)
except cx_Oracle.DatabaseError,msg:
    print "Failed: sql=%s" % sql, msg
    sys.exit()
print "Done.\n"

# check progress
print "Waiting for sync completing..."
while True:
    try:
        sql = "select sequence#,applied from v$archived_log order by sequence# desc"
        cur_backup.execute(sql)
    except cx_Oracle.DatabaseError,msg:
        print "Failed: sql=%s" % sql, msg
        sys.exit()
    rows = cur_backup.fetchmany()
    if rows[0][1] == "IN-MEMORY":
        break
    time.sleep(5)
    continue
print "Done.\n"


##
# create standby redo log on main.
##
# get max redo log group number on main.
print "Creating standby redo log on Primary Database..."
try:
    sql = "select max(group#) from v$log"
    cur_main.execute(sql)
except cx_Oracle.DatabaseError,msg:
    print "Failed: sql=%s" % sql, msg
    sys.exit()
res = cur_main.fetchone()
max_log_group_num = res[0]
# get online redo log group info and create one by one on main.
try:
    sql = "select bytes from v$log"
    cur_main.execute(sql)
except cx_Oracle.DatabaseError,msg:
    print "Failed: sql=%s" % sql, msg
    sys.exit()
rows = cur_main.fetchmany()
log_group_num = max_log_group_num + 1
for row in rows:
    try:
        sql = "alter database add standby logfile group %s size %s" % (log_group_num, row[0])
        cur_main.execute(sql)
    except cx_Oracle.DatabaseError,msg:
        print "Failed: sql=%s" % sql, msg
        sys.exit()
    log_group_num = log_group_num + 1
# Since one more standby redo log group should be created, create it.
try:
    sql = "alter database add standby logfile group %s size %s" % (log_group_num, row[0])
    cur_main.execute(sql)
except cx_Oracle.DatabaseError,msg:
    print "Failed: sql=%s" % sql, msg
    sys.exit()
print "Done.\n"

cur_main.close()
con_main.close()
cur_backup.close()
con_backup.close()


print "Complete.\n"
