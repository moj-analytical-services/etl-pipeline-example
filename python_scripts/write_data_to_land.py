import os
import gzip
import json

from datetime import datetime

from gluejobutils.s3 import (
    s3_path_to_bucket_key,
    s3_resource
)

from urllib.request import urlopen

from custom_functions import (
    unpack_data,
    write_dicts_to_jsonl_gz
)

from constants import (
    land_base_path,
    api_get
)

def main():
    # Get the run timestamp of this script - use it as the file partition (note that I remove milliseconds)
    run_timestamp = int(datetime.now().timestamp())

    # Request the API 1000 times
    data = []
    print_iter = 10
    for i in range(0, 1000):
        if i % print_iter == 0:
            print(f"Running the next {print_iter} calls to the API (i={i})")
        f = urlopen(api_get)
        api_out = f.readlines()[0]
        row = json.loads(api_out)
        
        new_row = unpack_data(row)
        new_row['index'] = i
        data.append(new_row)

    s3_out = os.path.join(land_base_path, 'random_postcodes', f'file_land_timestamp={run_timestamp}', f'random_postcodes_{run_timestamp}.jsonl.gz')
    print(f"compressing and writing data to: {s3_out}")
    write_dicts_to_jsonl_gz(data, s3_out)

if __name__ == '__main__':
    main()