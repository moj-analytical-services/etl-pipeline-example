import os
from datetime import datetime

from gluejobutils import s3

from etl_manager.utils import read_json

from constants import (
    land_base_path,
    raw_hist_base_path
)

from custom_functions import (
    read_jsonl_from_s3
)

def main():
    table_land_path = os.path.join(land_base_path, 'random_postcodes/')

    error = False
    meta = read_json('meta_data/raw/random_postcodes.json')
    colnames = [c['name'] for c in meta['columns']]

    # Get all partitions then test each one
    all_data_paths = s3.get_filepaths_from_s3_folder(table_land_path)
    if len(all_data_paths) == 0:
        raise ValueError(f"Was expecting data in land but nothing was found in the folder: {table_land_path}")
        
    for data_path in all_data_paths:
        print(f'TESTING {data_path}')
        data = read_jsonl_from_s3(data_path, compressed=True)
        
        # Let's say we always expect at least 100 records
        len_data = len(data)
        if len_data < 100:
            error = True
            print(f"TEST DATA SIZE: FAILED (size {len_data})")
        else:
            print(f"TEST DATA SIZE: PASSED (size {len_data})")
            
        # We might want to check the data against the our meta data (if we expect all the columns to exist)
        # If there is an error wait to test the rest of the data so you can see which other rows fail before raising an error
        error_str = ''
        for i, row in enumerate(data):
            col_mismatch = list(set(row.keys()).symmetric_difference(set(colnames)))
            if len(col_mismatch) > 0:
                error_str += f"row {i}: col mismatch: {col_mismatch.join(', ')}\n"
                error = True
        
        if error_str != '':
            print(error_str)
        
        if error:
            raise ValueError("Raising error due to one of the tests not passing. See log.")
        else:
            print("All tests passed!")
            print("Now writing to raw and deleting from land...")
            raw_hist_out = data_path.replace('s3://mojap-land/', 's3://mojap-raw-hist/')
            s3.copy_s3_object(data_path, raw_hist_out)
            s3.delete_s3_object(data_path)
            print("Done.")

if __name__ == '__main__':
    main()