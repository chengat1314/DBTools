# export data from Oracle to Local
import csv
import cx_Oracle
import sys
import time
import datetime

tableName = 'dg_bob_user_id'
printHeader = True # include column headers in each table output
arraysize = 1000

def ResultIter(cursor, output, arraysize=1000):
    #'An iterator that uses fetchmany to keep memory usage down'
    while True:
        res = cursor.fetchmany(arraysize)
        if not res:
            break
        writeIntoFile(output,res)

def writeIntoFile(output,res): 
    # output each table content to a separate CSV file 
    for row_data in res: # add table rows
        output.writerow(row_data)

def exportToFile(tableName,cursor):            
	csv_file_dest = tableName + ".csv"
	outputFile = open(csv_file_dest,'w') # 'wb'
	output = csv.writer(outputFile, dialect='excel')
	sql = "select * from " + tableName; 

	if printHeader: # add column headers if requested
	    cols = []
	    for col in cursor.description:
	        cols.append(col[0])
	    output.writerow(cols)
	#start fetch by batch arraysize per time
	ResultIter(cursor, output, arraysize)
	outputFile.close()

def main(tableName):
	conn = cx_Oracle.connect("UserName/Passwd@IP/DBName")
	curs = conn.cursor() 

	sql = "select * from " + tableName # get a list of all tables
	cursor = curs.execute(sql)
	exportToFile(tableName,cursor)
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

