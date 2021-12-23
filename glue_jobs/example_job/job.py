import sys
import os

# Standard set of inports for a glue job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F

# We've added gluejobutils as a package we can use in this script
from gluejobutils.s3 import read_json_from_s3

from gluejobutils.datatypes import align_df_to_meta

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'metadata_base_path', 'github_tag', 'snapshot_date'])

# Good practice to print out arguments for debugging
print("JOB SPECS...")
print(f"JOB_NAME: {args['JOB_NAME']}")
print(f"metadata_base_path: {args['metadata_base_path']}")
print(f"GITHUB_TAG: {args['github_tag']}")
print (f"SNAPSHOT_DATE: {args['snapshot_date']}")

# Init your spark script
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read in meta data
database_meta = read_json_from_s3(os.path.join(args['metadata_base_path'], "curated/database.json"))
random_postcodes_meta = read_json_from_s3(os.path.join(args['metadata_base_path'], "curated/random_postcodes.json"))
calculated_meta = read_json_from_s3(os.path.join(args['metadata_base_path'], "curated/calculated.json"))

# Read in the data
spark.read.json('s3://mojap-raw-hist/open_data/postcodes_example/').createOrReplaceTempView('postcodes')

# Do some spark transforms (not much to do here let's just add an extra fields)
postcodes = spark.sql("""
SELECT *, '{}' AS dea_version
FROM postcodes
""".format(args['github_tag']))

postcodes.createOrReplaceTempView('postcodes')

print (postcodes.columns) 

# Now let's create our calculated table
calculated = spark.sql("""
SELECT LOWER(european_electoral_region) as european_electoral_region, count(*) as n
FROM postcodes
GROUP BY LOWER(european_electoral_region)
""")

# Add the dea_version column this time using the spark df api (rather than SQL)
calculated = calculated.withColumn('dea_version', F.lit(args['github_tag']))

# Now use gluejobutils to make sure the metadata conforms and write to curated
postcodes = align_df_to_meta(postcodes, random_postcodes_meta)
postcodes.write.mode('overwrite').format('parquet').save("s3://alpha-curated-postcodes-example/database/random_postcodes/")

# A better way to define how the dataset is outputted is use the meta_data as seen below
# The calculated dataset always writes to the snapshot_date as a partition can do this by writing directly to partition
calculated = align_df_to_meta(calculated, calculated_meta, drop_columns=calculated_meta['partitions'])
calculated_out = os.path.join('s3://', database_meta['bucket'], database_meta['base_folder'], calculated_meta['location'], f"dea_snapshot_date={args['snapshot_date']}")
# End out path with a slash
if calculated_out[-1] != '/':
    calculated_out = calculated_out + '/'

calculated.write.mode('overwrite').format(calculated_meta['data_format']).save(calculated_out)

# Just a glue thing to add on the end of your gluejob
job.commit()