#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug  6 02:21:48 2017
Confidential
@author: pirwin
"""

from pydomo import Domo
from pydomo.datasets import DataSetRequest, Schema, Column, ColumnType, Policy
from pydomo.datasets import PolicyFilter, FilterOperator, PolicyType, Sorting
from pydomo.groups import CreateGroupRequest
from pydomo.streams import UpdateMethod, CreateStreamRequest
from pydomo.users import CreateUserRequest
from random import randint
import tempfile
import shutil
import os
import logging
import pandas as pd
import pyodbc
import time
import concurrent.futures
import asyncio

class DomoSDK:
    def __init__(self):
        # Docs: https://developer.domo.com/docs/domo-apis/getting-started
        # Create an API client on https://developer.domo.com
        # Initialize the Domo SDK with your API client id/secret
        # If you have multiple API clients you would like to use, simply initialize multiple Domo() instances
        client_id = r'8927c0c4-9d69-4d00-b7b3-6a228ee8b488'
        client_secret = r'4fdf56a78af8ac9910ed482f5ed170f46eaca70b65b30b957f25c56b72da7687'
        api_host = 'api.domo.com'

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logging.getLogger().addHandler(ch)

        self.domo = Domo(client_id, client_secret, api_host, logger_name='foo', logger_level=logging.INFO, use_https=True)
        self.logger = self.domo.logger
        self.streams = self.domo.streams
        
        
    def getSQL(path):
        print('Building Query...')
        with open(path, 'r') as myfile:
            #data=myfile.read().replace('\n', ' ').replace(';', ' ').replace('"', '\"').replace("'", "\'")
            data = ' '.join(myfile.read().split())
            return data
        
        
        
    def makeTempDir():
        print('Making Temp Directory...')
        tmp = tempfile.mkdtemp()
        return tmp
    
    

    def deleteTemp(tempdir):
        print('Deleting Temp Directory...')
        shutil.rmtree(tempdir)    
        
        
    def buildfilelist(directory):
        file_list = []
        for path, subdirs, files in os.walk(directory):
            for name in files:
                file_list.append(os.path.join(path, name))
        return file_list
    
    
        
    def listStreams(self):
        limit = 1000
        offset = 0
        stream_list = self.streams.list(limit, offset)
        return stream_list
        
    def createStream(self, name, schem):
        print('Creating Stream ' + name + '...')
        dsr = DataSetRequest()
        dsr.name = name
        dsr.schema = schem
        stream_request = CreateStreamRequest(dsr, UpdateMethod.REPLACE)
        stream = self.streams.create(stream_request)
        print('Stream ID: ' + str(stream.id))
        return stream
    
    
    def createExecution(self, strm):
        print('Creating execution...')
        execution = self.streams.create_execution(strm.id)
        return execution
    
    def deleteStream(self, stream):    
        self.streams.delete(stream.id)
        self.datasets.delete(stream.dataSet.id)
        
        
    def readData(sql, temp_dir, rowsper=100000):
        print('Reading data...')
        cnxn = pyodbc.connect('DRIVER={NetezzaSQL};SERVER=SRVDWHITP01;DATABASE=EDW_SPOKE;UID=pairwin;PWD=pairwin;TIMEOUT=0')   
        i = 0
        for chunk in pd.read_sql(sql, cnxn, chunksize=rowsper) :
            if i == 0:
                dtype_df = chunk.dtypes.reset_index()
                dtype_df.columns = ["Count", "Column Type"]
            chunk.to_csv(temp_dir+'\\file'+str(i)+'.gzip',index=False, compression='gzip', header=False)
            #print(temp_dir+'\\file'+str(i))
            i+=1
        print('Data read complete...')
        return dtype_df
    

        
        
    def uploadStream(self, execution, stream, filelist):
        print('Starting Upload')
        t = time.time()
        i = 0
        args = list()
        for file in filelist:
            i+=1
            args.append([stream.id, execution.id, i, file])
    
        def uploadPart(self, arglist):
            
            for i in range(5):
                    try:
                        
                        execution = self.streams.upload_part(arglist[0], arglist[1],arglist[2], csv = open(arglist[3], 'rb'))
                        
                        if execution is None:
                            raise Exception('Error uploading part ' + str(arglist[3]) + ' Retrying...')
                            
                    except Exception as e:
                        print(e)
                        #logging.warning(str(arglist[2]) + ' Failed on part ' + str(arglist[3]) + '... retrying in 5 sec')
                        time.sleep(5)
                        continue
                    break
        async def upload(self):
        
            with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
          
                loop = asyncio.get_event_loop()
                futures = [
                    loop.run_in_executor(
                        executor, 
                        uploadPart,
                        self,
                        arg
                    )
                    for arg in args
                ]
                for response in await asyncio.gather(*futures):
                    pass
        
        
        loop = asyncio.get_event_loop()
        loop.run_until_complete(upload())
        
            
        self.streams.commit_execution(stream.id, execution.id)
        print('Completed Stream Upload in ' + str(time.time()-t) + ' secs...')
    
    
    
    
    
    

    
    
    
    # try fetch many...
    #https://stackoverflow.com/questions/7555680/create-db-connection-and-maintain-on-multiple-processes-multiprocessing
    def readDataAsync(self, sql, temp_dir, rowsper=100000):    
        print('Reading data...')
        cnxn = pyodbc.connect('DRIVER={NetezzaSQL};SERVER=SRVDWHITP01;DATABASE=EDW_SPOKE;UID=pairwin;PWD=pairwin;TIMEOUT=0') 
            
        loop = asyncio.get_event_loop()
        
        
        def writePart(self, data, tdir):
            #print(data)
            global count 
            global dtype_df
            if count == 0:
                    dtype_df = data.dtypes.reset_index()
                    dtype_df.columns = ["Count", "Column Type"]
            #print(df)
            file = tdir + '\\file' + str(count)
            print(file)
            data.to_csv(file, index=False, compression='gzip', header=False)
            count += 1
        
        async def readChunk(tdir, rowsper):
        
            with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
          
                loop = asyncio.get_event_loop()
                futures = [
                    loop.run_in_executor(
                        executor, 
                        writePart,
                        self,
                        chunk,
                        temp_dir
                    )
                    for chunk in pd.read_sql(sql, cnxn, chunksize=rowsper)
                ]
                for response in await asyncio.gather(*futures):
                    pass
        
        loop.run_until_complete(readChunk(temp_dir, rowsper))
        


    def buildSchema(df):
        print('Building Schema...')
        sclist = list()
        for row in df.itertuples():
            if str(row[2]) == 'int64':
                sclist.append(Column(ColumnType.LONG, row[1]))
            elif str(row[2]) == 'float64': 
                sclist.append(Column(ColumnType.DECIMAL, row[1]))
            elif str(row[2]) == r'datetime64[ns]': 
                sclist.append(Column(ColumnType.DATETIME, row[1]))
            elif str(row[2]) == r'date64[ns]': 
                sclist.append(Column(ColumnType.DATE, row[1]))
            elif str(row[2]) == 'object': 
                sclist.append(Column(ColumnType.STRING, row[1]))
        return sclist

