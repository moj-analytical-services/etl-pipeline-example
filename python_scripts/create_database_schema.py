from etl_manager.meta import read_database_folder

def main():
    db = read_database_folder('meta_data/curated/')
    db.delete_glue_database()
    db.create_glue_database()

    db.refresh_all_table_partitions()

if __name__ == '__main__':
    main()