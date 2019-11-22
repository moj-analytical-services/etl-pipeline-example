import json
import gzip

from gluejobutils.s3 import (
    s3_path_to_bucket_key,
    s3_resource
)

# Lil function to take the api response and put into a tabular format
def unpack_data(data):
    new_dict = {}
    row = data['result']
    for c in row:
        if c != 'codes':
            new_dict[c] = row[c]
    for c in row["codes"]:
        new_dict["codes_" + c] = row["codes"][c]
    return new_dict

# Takes list of dicts, writes to it to a string as jsonl, compresses it and writes to S3
def write_dicts_to_jsonl_gz(data, s3_path):
    file_as_string = json.dumps(data[0])
    for d in data[1:]:
        file_as_string += '\n'
        file_as_string += json.dumps(d)
    b, k = s3_path_to_bucket_key(s3_path)
    compressed_out = gzip.compress(bytes(file_as_string, 'utf-8'))
    s3_resource.Object(b, k).put(Body=compressed_out)

# Reads in a jsonl from S3 (gziped or not)
def read_jsonl_from_s3(s3_path, encoding='utf-8', compressed=False) :
    """
    read a jsonl file from an s3 path
    """
    bucket, key = s3_path_to_bucket_key(s3_path)
    obj = s3_resource.Object(bucket, key)
    text = obj.get()['Body'].read()
    
    if compressed:
        split_text = gzip.decompress(text).decode(encoding).split('\n')
    else:
        split_text = text.decode(encoding).split('\n')
    
    data = []
    for t in split_text:
        data.append(json.loads(t))
        
    return data