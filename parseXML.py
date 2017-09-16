#!/usr/bin/python

import psycopg2
from config import config
 
def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
 
        # create a cursor
        cur = conn.cursor()
        
	# execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

	insSQL = "Insert into reqsmaster (reqid, attachcount, reqtitle, reqstatus, openings, active, createddate, org) Values \
			( %d,%d,%s,%s,%d,%s,%t,%s) Values ( % \
			ReqID,AttachCount,ReqTitle,ReqStatus,Openings,Active,CreatedDate,Org )"
	print (insSQL)
 
        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
	from xml.etree import ElementTree

	with open ('req.xml','rt') as f:
		tree = ElementTree.parse(f)

	rowCount = 0
	ReqID = 0
	AttachCount = 0
	ReqTitle = ''
	ReqStatus = ''
	Openings = 0
	Active = 0
	CreatedDate = ''
	Org = ''
	foundRow = False
	parseREQ = False
	itemVal = 0 

	for node in tree.iter():
		if (node.tag == '{urn:schemas-microsoft-com:office:spreadsheet}Row'):
			# We found the requirement. Store the requirement details to the variables 
			# in sequence, till we hit the next row.
			rowCount = rowCount + 1 
			foundRow = True
			parseREQ = False
			ReqID = 0
			AttachCount = 0
			ReqTitle = ''
			ReqStatus = ''
			Openings = 0
			Active = 0
			CreatedDate = ''
			Org = ''
			itemVal = 0 
		else:
			parseREQ = True
			foundRow = False

		if ( parseREQ and node.tag == '{urn:schemas-microsoft-com:office:spreadsheet}Data' ):
			itemVal = itemVal + 1
			if ( itemVal == 1 ):
				ReqID = node.text
			elif ( itemVal == 2 ):
				AttachCount = node.text
			elif ( itemVal == 3 ):
				ReqTitle = node.text
			elif ( itemVal == 4 ):
				ReqStatus = node.text
			elif ( itemVal == 5 ):
				Openings = node.text
			elif ( itemVal == 6 ):
				Active = node.text
			elif ( itemVal == 7 ):
				CreatedDate = node.text
	 		elif ( itemVal == 8 ):
				Org = node.text
			else:
				print ('Processing requirement %d ' % rowCount)
		if ( parseREQ and itemVal == 8 ):
			# print (ReqID,AttachCount,ReqTitle,ReqStatus,Openings,Active,CreatedDate,Org)
			# print("INSERT INTO reqsmaster (reqid, attachcount, reqtitle, reqstatus, openings, active, createddate, org) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')" %  (ReqID,AttachCount,ReqTitle,ReqStatus,Openings,Active,CreatedDate,Org))
			if ( ReqID != 'ReqID' ):
				insSQL = "INSERT INTO reqsmaster (reqid, attachcount, reqtitle, reqstatus, openings, active, createddate, org) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)" 			
				data = (ReqID,AttachCount,ReqTitle,ReqStatus,Openings,Active,CreatedDate,Org,)
				cur.execute(insSQL, data )
				conn.commit()

	print('The Total Requirements are %d ' % rowCount)


     # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
 
 
if __name__ == '__main__':
    connect()




