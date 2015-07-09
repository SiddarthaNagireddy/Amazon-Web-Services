'''
Created on Jul 8, 2015

@author: sid.reddy
'''
import boto3 
from boto3.s3 import transfer
import os
from shutil import copyfileobj
import gzip

 
## Source Bucket
sourceBucket = 'sh-datadumps'

## Folder Name 
folderName = 'indeed/resumes/2015-06/'


class s3Download():
    
    def __init__(self,sourceBucket,folderName=None):
        self.s3 = boto3.resource('s3')
        self.sourceBucket = sourceBucket
        self.folderName = folderName
        self.bucketName = self.s3.Bucket(sourceBucket)
        self.client = boto3.client('s3')

    def downloadFiles(self,fileList,downloadPath=None):
        if downloadPath == None:
            downloadPath = ''
        for fileName in fileList:
            print "downloading %s"%fileName
            transfer.S3Transfer(client=self.client).download_file(self.sourceBucket, 
                                                                  key=fileName, 
                                                                  filename=downloadPath+fileName.split('/')[-1])

    def listFiles(self):
        fileList = []
        for fileName in self.s3.Bucket(self.sourceBucket).objects.all():
            if self.folderName == None:
                if self.folderName in fileName.key:
                    fileList.append(fileName.key)
            else:
                ''' Generate a list of all files in the Bucket '''
                fileList.append(fileName.key)
        return fileList
    
    def decompress(self,fileName,iPath,oPath):
        inp = gzip.open(iPath+fileName, 'rb')
        out = open(oPath+fileName.strip('.gz'),'w')
        copyfileobj(inp, out)


if __name__ == '__main__':
    testVar = s3Download(sourceBucket,folderName)
    testVar.downloadFiles(testVar.listFiles(), downloadPath='~/downloadedFiles')

