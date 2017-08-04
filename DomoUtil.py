# -*- coding: utf-8 -*-
"""
Created on Tue Jul 25 09:33:08 2017

@author: pairwin
"""

import os
import pyodbc
import pandas as pd
import tempfile
import shutil
import argparse
from pydomo import Domo
from pydomo.datasets import DataSetRequest, Schema, Column, ColumnType
from pydomo.streams import UpdateMethod, CreateStreamRequest
import pydomo.streams as streams
import logging
import asyncio
#import aioodbc
import concurrent.futures
import time


count = 0
dtype_df = None



def setupDomo(cid, csec):
    api_host = 'api.domo.com'
    use_https = True
    logger_name = 'foo'
    logger_level = logging.WARNING
    global domo
    domo = Domo(cid, csec, api_host, use_https, logger_name, logger_level)
    

def buildfilelist(directory):
    file_list = []
    for path, subdirs, files in os.walk(directory):
        for name in files:
            file_list.append(os.path.join(path, name))
    return file_list

def createStream(name, schem):
    print('Creating Stream ' + name + '...')
    dsr = DataSetRequest()
    dsr.name = name
    dsr.schema = schem
    stream_request = CreateStreamRequest(dsr, UpdateMethod.REPLACE)
    stream = domo.streams.create(stream_request)
    print('Stream ID: ' + str(stream.id))
    return stream

def searchStream(name):
    print('Creating stream...')
    stream_prop = 'dataSource: ' + name
    stream = streams.search(stream_prop)
    return stream

def deleteStream(stream):    
    domo.streams.delete(stream.id)
    domo.datasets.delete(stream.dataSet.id)

def createExecution(strm):
    print('Creating execution...')
    execution = domo.streams.create_execution(strm.id)
    return execution

def uploadPart(arglist):
    dmo = arglist[0]
    
    for i in range(5):
            try:
                
                execution = dmo.streams.upload_part(arglist[1], arglist[2],arglist[3], csv = open(arglist[4], 'rb'))
                    
            except Exception as e:
                print(e)
                #logging.warning(str(arglist[2]) + ' Failed on part ' + str(arglist[3]) + '... retrying in 5 sec')
                time.sleep(5)
                continue
            break
    
    
def uploadStream(execution, stream, filelist):
    print('Starting Upload')
    t = time.time()
    i = 0
    args = list()
    for file in filelist:
        i+=1
        args.append([domo, stream.id, execution.id, i, file])

    
    async def upload():
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
      
            loop = asyncio.get_event_loop()
            futures = [
                loop.run_in_executor(
                    executor, 
                    uploadPart,
                    arg
                )
                for arg in args
            ]
            for response in await asyncio.gather(*futures):
                pass
    
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(upload())
    
        
    domo.streams.commit_execution(stream.id, execution.id)
    print('Completed Stream Upload in ' + str(time.time()-t) + ' secs...')

def makeTempDir():
    print('Making Temp Directory...')
    tmp = tempfile.mkdtemp()
    return tmp

def deleteTemp(tempdir):
    print('Deleting Temp Directory...')
    shutil.rmtree(tempdir)

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


def writePart(data, tdir):
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



# try fetch many...
#https://stackoverflow.com/questions/7555680/create-db-connection-and-maintain-on-multiple-processes-multiprocessing
def readDataAsync(sql, temp_dir, rowsper=100000):    
    print('Reading data...')
    cnxn = pyodbc.connect('DRIVER={NetezzaSQL};SERVER=SRVDWHITP01;DATABASE=EDW_SPOKE;UID=pairwin;PWD=pairwin;TIMEOUT=0') 
        
    loop = asyncio.get_event_loop()


    async def readChunk(tdir, rowsper):
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
      
            loop = asyncio.get_event_loop()
            futures = [
                loop.run_in_executor(
                    executor, 
                    writePart,
                    chunk,
                    temp_dir
                )
                for chunk in pd.read_sql(sql, cnxn, chunksize=rowsper)
            ]
            for response in await asyncio.gather(*futures):
                pass
    
    loop.run_until_complete(readChunk(temp_dir, rowsper))

    


        
        
        
def getSQL(path):
    print('Building Query...')
    with open(path, 'r') as myfile:
        #data=myfile.read().replace('\n', ' ').replace(';', ' ').replace('"', '\"').replace("'", "\'")
        data = ' '.join(myfile.read().split())
        return data

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

def listStreams():
    limit = 1000
    offset = 0
    stream_list = domo.streams.list(limit, offset)
    return stream_list


def main(args):
    client_id = r'yourclientid'
    client_secret = r'yourclientsecret'
    args = args
    try:
        
        if args.rows is not None:
            rowsper = int(args.rows)
        else:
            rowsper = 100000
        
        temp_dir = makeTempDir()
        setupDomo(client_id, client_secret)
        
    
        
        if args.sqlfile is not None:
            sql = getSQL(args.sqlfile)    
#            schemadf = readData(sql, temp_dir, rowsper)
            readDataAsync(sql, temp_dir, rowsper)
            fl = buildfilelist(temp_dir)
        
        if args.name is not None:
            name = args.name
            
        if args.exec is not None:
            dataSourceId = args.exec
            strlst = listStreams()
            for i in range(len(strlst)):
                if strlst[i].dataSet.id == dataSourceId:
                    strm_id = strlst[i].id

            strm = domo.streams.get(strm_id) #stream id
            print('Updating ' + strm.dataSet.name)
            exe = createExecution(strm)
            uploadStream(exe, strm, fl)
            
        if args.delete is not None:
            dataSourceId = args.delete

            strlst = listStreams()
            for i in range(len(strlst)):
                if strlst[i].dataSet.id == dataSourceId:
                    strm_id = strlst[i].id

            strm = domo.streams.get(strm_id) #stream id
            print('Deleting ' + strm.dataSet.name)
            deleteStream(strm)
            
        
        if args.create:
            SCHEMA = Schema(buildSchema(dtype_df))  
            strm = createStream(name, SCHEMA)
            exe = createExecution(strm)
            uploadStream(exe, strm, fl)
    
    except Exception as err:
        print(err)

        
    finally:
        deleteTemp(temp_dir)
    
    
    
    
    
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sqlfile", help="SQL File for processing")
    parser.add_argument("-c", "--create", help="Create Stream", action="store_true")
    parser.add_argument("-e","--exec", help="Execute existing Stream by giving the dataSourceId")
    parser.add_argument("-n", "--name", help="Name of Stream you want to create")
    parser.add_argument("-d", "--delete", help="Id of Stream you want to delete")
    parser.add_argument("-r", "--rows", help="Rows per chunk (default is 100000)")
    args = parser.parse_args()
    main(args)    

    
    
#setupDomo()
#temp_dir = makeTempDir()    
#name = 'TEST_STREAM_PI'
#sql = getSQL(r'C:\users\pairwin\Desktop\testsql.sql')
#schemadf = readData(sql, temp_dir, 10000)
#readDataAsync(sql, temp_dir)
#fl = buildfilelist(temp_dir)
#SCHEMA = Schema(buildSchema(schemadf))
#strm = createStream(name, SCHEMA)
#
#strlst = listStreams()
#for i in range(len(strlst)):
#    if strlst[i].dataSet.id == 'df66cb75-d2ea-47ac-a4aa-8de48e22c1b3':
#        strm_id = strlst[i].id
#strm = domo.streams.get(strm_id) #stream id
#
#exe = createExecution(strm)
#arglist = [strm.id, exe.id, 1, r'C:\Temp\tmp4rignsc0\file0.gzip' ]
#
#for i in range(20):
#    execution = domo.streams.upload_part(arglist[0], arglist[1],arglist[2], csv = open(arglist[3], 'rb'))
#    print(execution)
#    
#
#uploadPart(arglist)
#domo.streams.commit_execution(strm.id, exe.id)
#deleteTemp(temp_dir)
#domo.streams.delete(strm.id)
#domo.datasets.delete(strm.datasets.id)