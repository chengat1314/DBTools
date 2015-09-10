# export data from Oracle to Local
import csv
import cx_Oracle
import sys
import time
import datetime

tableName = 'dg_bob_user_id_test'
printHeader = True # include column headers in each table output
arraysize = 1000 

def loadIntoTable(tableName,cursor):      
	fileName = 'dg_bob_user_id.csv'
	list = []
	f = open(fileName,'r')
	for line in f:
	    pair = line.split(',')
	    list.append((int(pair[0]),int(pair[1]))) #tuple(pair)

	# customize the column names as columnNames in the table 
	print len(list)
	cursor.prepare('insert into ' + tableName + ' (CUST_ID,CLUS_ID) values (:1, :2)')
	cursor.executemany(None, list) 

	f.close() 

def main(tableName):
	conn = cx_Oracle.connect("userName/passwd@ip/DBName")
	curs = conn.cursor()  
	loadIntoTable(tableName,curs)
	conn.commit()
	conn.close()

if __name__ == '__main__':
    if len(sys.argv)==2:
    	tableName=sys.argv[1]
    print 'Start at ' + str(datetime.datetime.now())
    main(tableName)
    print 'End at ' + str(datetime.datetime.now())

'''
create table dg_bob_user_id_test
as select * from dg_bob_user_id
where rownum<20; 
'''
