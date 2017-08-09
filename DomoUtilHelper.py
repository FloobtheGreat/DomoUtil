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
from datetime import datetime
import configparser

class DomoSDK:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        client_id = config['DEFAULT']['clientid']
        client_secret = config['DEFAULT']['clientsecret']
        api_host = config['DEFAULT']['api_host']
        self.databasecon = config['DEFAULT']['databasecon']
        # Docs: https://developer.domo.com/docs/domo-apis/getting-started
        # Create an API client on https://developer.domo.com
        # Initialize the Domo SDK with your API client id/secret
        # If you have multiple API clients you would like to use, simply initialize multiple Domo() instances

        ch = logging.FileHandler('DomoUtilLog.log', 'w')
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        
        logging.getLogger().addHandler(ch)

        self.domo = Domo(client_id, client_secret, api_host, logger_name='BPS Domo Utility', logger_level=logging.INFO, use_https=True)
        self.logger = self.domo.logger
        self.streams = self.domo.streams
        self.datasets = self.domo.datasets
        self.count = 0
        self.logger.info('Initiating Domo Instance')
        self.dataname = None
        
        
    def getSQL(self, path):
        self.logger.info('Building Query...')
        with open(path, 'r') as myfile:
            #data=myfile.read().replace('\n', ' ').replace(';', ' ').replace('"', '\"').replace("'", "\'")
            data = ' '.join(myfile.read().split())
            return data
        
        
        
    def makeTempDir(self):
        self.logger.info('Making Temp Directory...')
        tmp = tempfile.mkdtemp()
        return tmp
    
    

    def deleteTemp(self, tempdir):
        self.logger.info('Deleting Temp Directory...')
        shutil.rmtree(tempdir)    
        
        
    def buildfilelist(self, directory):
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
        self.logger.info('Creating Stream ' + name + '...')
        dsr = DataSetRequest()
        dsr.name = name
        self.dataname = name
        dsr.schema = schem
        stream_request = CreateStreamRequest(dsr, UpdateMethod.REPLACE)
        stream = self.streams.create(stream_request)
        self.logger.info('Stream ID: ' + str(stream.id))
        return stream
    
    
    def createExecution(self, strm):
        self.logger.info('Creating execution...')
        execution = self.streams.create_execution(strm.id)
        return execution
    
    def deleteStream(self, stream):    
        self.streams.delete(stream.id)
        self.datasets.delete(stream.dataSet.id)
        
        
    def readData(self, sql, temp_dir, rowsper=100000):
        self.logger.info('Reading data...')
        cnxn = pyodbc.connect(self.databasecon)   
        i = 0
        for chunk in pd.read_sql(sql, cnxn, chunksize=rowsper) :
            if i == 0:
                dtype_df = chunk.dtypes.reset_index()
                dtype_df.columns = ["Count", "Column Type"]
            chunk.to_csv(temp_dir+'\\file'+str(i)+'.gzip',index=False, compression='gzip', header=False)
            self.logger.info(temp_dir+'\\file'+str(i))
            i+=1
        self.logger.info('Data read complete...')
        return dtype_df
    

    def uploadPart(self, arglist):            
        for i in range(5):
                try:
                    
                    execution = self.streams.upload_part(arglist[0], arglist[1],arglist[2], csv = open(arglist[3], 'rb'))
                    
                    if execution is not None:
                        self.logger.info('Response: ' + str(execution))
                        
                except Exception as e:
                    self.logger.error(e)
                    #logging.warning(str(arglist[2]) + ' Failed on part ' + str(arglist[3]) + '... retrying in 5 sec')
                    time.sleep(5)
                    continue
                break    
        
    def uploadStream(self, execution, stream, filelist):
        self.logger.info('Starting Upload')
        t = time.time()
        i = 0
        args = list()
        for file in filelist:
            i+=1
            args.append([stream.id, execution.id, i, file])
    
        
                
        async def upload(self, args):
        
            with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
          
                loop = asyncio.get_event_loop()
                futures = [
                    loop.run_in_executor(
                        executor, 
                        self.uploadPart,
                        arg
                    )
                    for arg in args
                ]
                for response in await asyncio.gather(*futures):
                    pass
        
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(upload(self, args))
        except Exception as e:
            self.logger.error(e)
        
            
        self.streams.commit_execution(stream.id, execution.id)
        self.logger.info('Completed Stream Upload in ' + str(time.time()-t) + ' secs...')
    
    def writePart(self, data, tdir):
        #print(data)
        
        if self.count == 0:
                self.dtype_df = data.dtypes.reset_index()
                self.dtype_df.columns = ["Count", "Column Type"]
        #print(df)
        file = tdir + '\\file' + str(self.count)
        self.logger.info(file)
        data.to_csv(file, index=False, compression='gzip', header=False)
        self.count += 1
    

    
    # try fetch many...
    #https://stackoverflow.com/questions/7555680/create-db-connection-and-maintain-on-multiple-processes-multiprocessing
    def readDataAsync(self, sql, temp_dir, rowsper=100000):    
        self.logger.info('Reading data...')
        cnxn = pyodbc.connect('DRIVER={NetezzaSQL};SERVER=SRVDWHITP01;DATABASE=EDW_SPOKE;UID=pairwin;PWD=pairwin;TIMEOUT=0') 
            
        loop = asyncio.get_event_loop()
        
        
        
        
        async def readChunk(self, tdir, rowsper):
        
            with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
          
                loop = asyncio.get_event_loop()
                futures = [
                    loop.run_in_executor(
                        executor, 
                        self.writePart,
                        chunk,
                        temp_dir
                    )
                    for chunk in pd.read_sql(sql, cnxn, chunksize=rowsper)
                ]
                for response in await asyncio.gather(*futures):
                    pass
        
        loop.run_until_complete(readChunk(self, temp_dir, rowsper))
        


    def buildSchema(self, dtype_df):
        self.logger.info('Building Schema...')
        sclist = list()
        for row in dtype_df.itertuples():
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

    def setName(self, nm):
        self.dataname = nm


    def closeLogger(self):
        logging.shutdown()
        shutil.copy2('DomoUtilLog.log', 'logs/DomoUtilLog'+ str(self.dataname) + '_' + datetime.now().strftime('%Y%m%d_%H_%M') + '.log')
        os.remove('DomoUtil.log')