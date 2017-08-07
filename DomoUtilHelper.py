#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug  6 02:21:48 2017

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
import logging

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
        
        
    def makeTempDir():
        print('Making Temp Directory...')
        tmp = tempfile.mkdtemp()
        return tmp
    
    

    def deleteTemp(tempdir):
        print('Deleting Temp Directory...')
        shutil.rmtree(tempdir)    
        
        
        
    def createStream(self, name, schem):
        streams = self.domo.streams
        print('Creating Stream ' + name + '...')
        dsr = DataSetRequest()
        dsr.name = name
        dsr.schema = schem
        stream_request = CreateStreamRequest(dsr, UpdateMethod.REPLACE)
        stream = streams.create(stream_request)
        print('Stream ID: ' + str(stream.id))
        return stream