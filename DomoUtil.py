# -*- coding: utf-8 -*-
"""
Created on Tue Jul 25 09:33:08 2017
Confidential
@author: pairwin
"""

import DomoUtilHelper as dmo
import argparse
from pydomo import Schema

count = 0
dtype_df = None







def searchStream(name):
    print('Creating stream...')
    stream_prop = 'dataSource: ' + name
    stream = streams.search(stream_prop)
    return stream





def main(args):
    
    args = args
    try:
        
        if args.rows is not None:
            rowsper = int(args.rows)
        else:
            rowsper = 100000
        
        domo = dmo.domoSDK()
        temp_dir = domo.makeTempDir()
        
    
        
        if args.sqlfile is not None:
            sql = domo.getSQL(args.sqlfile)    
#            schemadf = readData(sql, temp_dir, rowsper)
            domo.readDataAsync(sql, temp_dir, rowsper)
            fl = domo.buildfilelist(temp_dir)
        
        if args.name is not None:
            name = args.name
            
        if args.exec is not None:
            dataSourceId = args.exec
            strlst = domo.listStreams()
            for i in range(len(strlst)):
                if strlst[i].dataSet.id == dataSourceId:
                    strm_id = strlst[i].id

            strm = domo.streams.get(strm_id) #stream id
            print('Updating ' + strm.dataSet.name)
            exe = domo.createExecution(strm)
            domo.uploadStream(exe, strm, fl)
            
        if args.delete is not None:
            dataSourceId = args.delete

            strlst = domo.listStreams()
            for i in range(len(strlst)):
                if strlst[i].dataSet.id == dataSourceId:
                    strm_id = strlst[i].id

            strm = domo.streams.get(strm_id) #stream id
            print('Deleting ' + strm.dataSet.name)
            domo.deleteStream(strm)
            
        
        if args.create:
            SCHEMA = Schema(domo.buildSchema(dtype_df))  
            strm = domo.createStream(name, SCHEMA)
            exe = domo.createExecution(strm)
            domo.uploadStream(exe, strm, fl)
    
    except Exception as err:
        print(err)

        
    finally:
       domo.deleteTemp(temp_dir)
    
    
    
    
    
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