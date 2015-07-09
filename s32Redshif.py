'''
Created on Jul 9, 2015

@author: sid.reddy
'''

import sys
from boto.s3 import connect_to_region
from boto.s3.connection import Location
import csv
import itertools
import psycopg2

''' 
    Utility Class '''
    
class UTIL():
    
    def __init__(self):
        self.UTILS = s3DownloadPath.split('/')
    
    def bucket_name(self):
        self.BUCKET_NAME = self.UTILS[0]
        return self.BUCKET_NAME
    
    def path(self):
        self.PATH = ''
        offset = 0
        for value in self.UTILS:
            if offset == 0:
                offset += 1
            else:
                self.PATH = self.PATH + value + '/'
        return self.PATH[:-1]

''' 
    Sniff a part of file on S3 to
    generate schema '''
   
def getDataInMemory():
    DATA = ''
    conn = connect_to_region(Location.USWest2,aws_access_key_id = awsKey,
        aws_secret_access_key = awsSecretKey,
        is_secure=False,host='s3-us-west-2.amazonaws.com'
        )
    ut = UTIL()
    BUCKET_NAME = ut.bucket_name()
    PATH = ut.path()
    filelist = conn.lookup(BUCKET_NAME)
    
    ''' 
        Fecth part of the data from S3 '''
    
    for path in filelist:
        if PATH in path.name:
            DATA = path.get_contents_as_string(headers={'Range': 'bytes=%s-%s' % (0,100000000)}) 
            
    return DATA


''' 
    Traverse part of file to 
    generate table schema '''

def traverseData():
    data = getDataInMemory()
    createTableQuery = 'CREATE TABLE ' + redshiftSchema + '.' + tableName + '( '
    processedData = data[3:].split('\n')
    csvData = csv.reader(processedData,delimiter=',')
    counter,varChar,number = 0,0,0
    columnType = []
    
    ''' 
        Get Column count and names '''
    for line in csvData:
        numberOfColumns = len(line)
        columnNames = line
        break;
    
    ''' 
        clean Column Names '''
    a = 0
    for removeSpace in columnNames:
        tempHolder = removeSpace.split(' ')
        temp1 = ''
        for x in tempHolder:
            temp1 = temp1 + x 
        columnNames[a] = temp1
        a = a + 1
    
    ''' 
        Get Column DataTypes '''
    # print(numberOfColumns,columnNames,counter)
    # print(numberOfColumns)
    i,j,a= 0,500,0 
    while counter < numberOfColumns:
        for column in itertools.islice(csvData,i,j+1):
            if column[counter].isdigit():
                number = number + 1
            else:
                varChar = varChar + 1
        if number == 501:
            columnType.append('INTEGER')
            # print('I CAME IN')
            number = 0
        else:
            columnType.append('VARCHAR(2500)')
            varChar = 0
        counter = counter + 1
        # print(counter)
    
    counter = 0
    ''' 
        Build schema '''
    while counter < numberOfColumns:
        if counter == 0:
            createTableQuery = createTableQuery + columnNames[counter] + ' ' + columnType[counter] + ' NOT NULL,'
        else:
            createTableQuery = createTableQuery + columnNames[counter] + ' ' + columnType[counter] + ' ,'
        counter += 1
    createTableQuery = createTableQuery[:-2]+ ')'
    
    return createTableQuery

''' 
    Run the copy command '''
def copyCommand():
    s3Path = 's3://' + s3DownloadPath
    copyCommand = "COPY " + redshiftSchema + "." + tableName+" from '"+s3Path+"' credentials 'aws_access_key_id="+awsKey+";aws_secret_access_key="+awsSecretKey+"' REGION 'us-west-2' csv delimiter ',' ignoreheader as 1 TRIMBLANKS maxerror as 500"
    return copyCommand

''' 
    Connect to Redshift and execute Commands '''
def s3toRedshift():
    conn = psycopg2.connect("dbname='XXX' port='5439' user='XXX' host='XXXX' password='XXXX'")
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS '+ redshiftSchema + "." + tableName)
    schema = traverseData()
    print(schema)
    cursor.execute(schema)
    COPY = copyCommand()
    print(COPY)
    cursor.execute(COPY)
    conn.commit()


if __name__ == "__main__":  
    ''' 
        Arguments '''
    awsKey = sys.argv[1]
    awsSecretKey = sys.argv[2]
    s3DownloadPath = sys.argv[3]
    redshiftSchema = sys.argv[4]
    tableName = sys.argv[5] 
    s3toRedshift()