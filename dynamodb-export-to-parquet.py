# DynamoDB Export to Parquet
#
# Arguments:
# Run from console:
#   Key                  Value
#   --------------------------------------------------------------------------------------------------------------------------------
#   --backup_location    s3://your-bucket/raw/YourTable/2017-10-16-08-00-00/
#   --output_location    s3://your-bucket/parquet/YourTable/2017-10-16-08-00-00/
#
# From Python Lambda
#   glue_client.start_job_run(
#       JobName="DynamoBackupToParquet",
#       Arguments={"--backup_location": "s3://YOUR/DYNAMODB/BACKUP/FOLDER", "--output_location": "s3://YOUR/PARQUET/FOLDER"}
#   )
#
# Strategy:
#   * Read in raw string data from backup
#   * Load each item into a Python dict using DynamoItemDecoder as an object_hook.
#   * Convert all keys from CamelCase or mixedCase to snake_case (see comment on convert_mixed_case_to_snake_case)
#   * dump back to JSON
#   * Load data into a DynamicFrame
#   * Convert to Parquet and write to S3
import sys
import re

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from boto.compat import json
from boto.dynamodb.types import Dynamizer
from boto.dynamodb.types import is_str
from pyspark.context import SparkContext

######################## Classes and Functions ########################


class DynamoItemDecoder(Dynamizer):
  # Slightly modified from the parent class.
  #
  # Removed the check for DynamoDB type case ("S" vs "s") as backups from Data
  # Pipelines fail this check.
  def decode(self, attr):
    try:
      if len(attr) > 1 or not attr or is_str(attr):
          return attr
    except TypeError:
      return attr
    dynamodb_type = list(attr.keys())[0]
    try:
        decoder = getattr(self, '_decode_%s' % dynamodb_type.lower())
    except AttributeError:
        return attr
    return decoder(attr[dynamodb_type])
  # Sets can't be dumped back into JSON natively. so convert to lists.
  # We make no modifications to set memberships in this Job so there's no risk

  def _decode_ns(self, attr):
      return map(self._decode_n, attr)

  def _decode_ss(self, attr):
    return map(self._decode_s, attr)


DYNAMO_ITEM_DECODER = DynamoItemDecoder()


def remove_dynamo_types(item):
  try:
    item_dictionary = json.loads(item, object_hook=DYNAMO_ITEM_DECODER.decode)
    new_dictionary = {}
    for key, value in item_dictionary.iteritems():
        new_key = convert_mixed_case_to_snake_case(key)
        new_dictionary[new_key] = value
    return json.dumps(new_dictionary)
  except json.JSONDecodeError:
    return None

# Presto / Athena has issues querying column names with mixed cases. The naive
# approach is to just make everything lower case, but I find that hard to read.
# Instead let's convert to snake case to keep column names readable.


def convert_mixed_case_to_snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def flattenPattern(pattern):
    newPattern = {}
    if type(pattern) is list:
        pattern = pattern[0]

    if type(pattern) is not str:
        for key, value in pattern.items():
            if type(value) in (list, dict):
                returnedData = flattenPattern(value)
                for i,j in returnedData.items():
                        if key == "Activities":
                            newPattern[i] = j
                        else:
                            newPattern[key + "." + i] = j
            else:
                newPattern[key] = value


    return newPattern 

######################## Script Start ########################
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'backup_location',
    'output_location'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_dir = args['backup_location']
output_dir = args['output_location']

# Read in raw data as simple text lines
#
# Files starting with underscores are ignored (like "_SUCCESS"), however, a manifest
# JSON file is always present in an export. The names of the actual table files
# exported by Hadoop are UUIDs (Hexadecimal strings). We can exclude the manifest
# (and thus preventing the "mandatory" from ending up as a column in our DB) by excluding
# all files beginning with "m"
raw_lines = spark.read.text("%s/[^m]*" % input_dir)

# Load the raw JSON into Dictionaries while removing DynamoDB type information,
# dump back out to JSON.
#
# There's an optimization here that I haven't been able to get to work where we
# can save dumping a dict back out to JSON AND preserve the schema.
#
# It would look something like this:
# df = raw.rdd.map(lambda x: json.loads(x.value, object_hook=DynamoItemDecoder().decode))
#             .map(lambda x: Row(**x)).toDF()
#
# I get cryptic errors whenever I run that line on a Development endpoint in a
# pyspark session ¯\_(ツ)_/¯
raw_items = raw_lines.rdd.map(
    lambda x: remove_dynamo_types(x.value)).filter(lambda x: x)

# Read in the raw_items as JSON. Schema inference for the win!
raw_items_df = spark.read.json(raw_items)

flatten_items_df = flattenPattern(raw_items_df);
# Load items into a Dataframe so we can go up one more abstraction level into
# a DynamicFrame which is Glue's abstraction of choice.
items = DynamicFrame.fromDF(
    raw_items_df,
    glueContext,
    "items"
)

# Write out to S3 in Parquet
datasink = glueContext.write_dynamic_frame.from_options(
    frame=items,
    connection_type="s3",
    connection_options={
        "path": output_dir},
    format="parquet",
    transformation_ctx="datasink"
)

job.commit()
