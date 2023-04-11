
from __future__ import print_function
import base64
import json
import datetime
import fastavro
import avro.schema
import pickle
import avro.datafile
import avro.io
from avro.io import DatumWriter, DatumReader
import io
from avro_json_serializer import avro, AvroJsonSerializer
from random import randrange


 
# Signature for all Lambda functions that user must implement
def lambda_handler(firehose_records_input, context):
    print("Received records for processing from DeliveryStream: " + firehose_records_input['deliveryStreamArn']
          + ", Region: " + firehose_records_input['region']
          + ", and InvocationId: " + firehose_records_input['invocationId'])
 
    # Create return value.
    firehose_records_output = {'records': []}
 
    # Create result object.
    # Go through records and process them
    print("Whole data set")
    print(firehose_records_input)
    for firehose_record_input in firehose_records_input['records']:
        # Get user payload
        print("Base 64 encoded record")
        print(firehose_record_input['data'])
        payload = base64.b64decode(firehose_record_input['data']) 
        print("Base 64 decoded record")
        print(payload)
        print("apply deserialization to each record ")
        message_buf0= io.BytesIO(payload)
        reader0 = avro.datafile.DataFileReader(message_buf0, avro.io.DatumReader())
        datatoinsert=None
        partition_keys=None
        for thing in reader0:
            print(thing)
            thing['full_name']=thing['first_name']+thing['last_name']
            partition_keys = {"full_name": thing['full_name'] }
            datatoinsert=json.dumps(thing).encode('utf-8')
            print(datatoinsert)  
            print("Partition Keys")
            print(partition_keys)
        reader0.close()
        
        # Create output Firehose record and add modified payload and record ID to it.
        firehose_record_output = {}

        # partition_keys = {"full_name": json_value['first_name']
        #                   }
        # print("Partition Keys")
        # print(partition_keys)
        # Create output Firehose record and add modified payload and record ID to it.
        firehose_record_output = {'recordId': firehose_record_input['recordId'],
                                  'data': base64.b64encode(datatoinsert).decode('utf-8'),
                                  'result': 'Ok',
                                  'metadata': { 'partitionKeys': partition_keys }
                                  }
        datatoinsert=None
        partition_keys=None
        # Must set proper record ID
        # Add the record to the list of output records.
 
        firehose_records_output['records'].append(firehose_record_output)
 
    # At the end return processed records
    return firehose_records_output
