import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer_trusted
customer_trusted_node1748092945603 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1748092945603")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1748092918763 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1748092918763")

# Script generated for node SQL Query
SqlQuery548 = '''
select distinct
    cust_t.customername,
    cust_t.email,
    cust_t.phone,
    cust_t.birthday,
    cust_t.serialnumber,
    cust_t.registrationdate,
    cust_t.lastupdatedate,
    cust_t.sharewithresearchasofdate,
    cust_t.sharewithpublicasofdate,
    cust_t.sharewithfriendsasofdate
from 
    accelerometer_trusted acc_t JOIN customer_trusted cust_t on acc_t.user = cust_t.email
'''
SQLQuery_node1748093022155 = sparkSqlQuery(glueContext, query = SqlQuery548, mapping = {"customer_trusted":customer_trusted_node1748092945603, "accelerometer_trusted":accelerometer_trusted_node1748092918763}, transformation_ctx = "SQLQuery_node1748093022155")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1748093022155, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1748090981230", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1748093491483 = glueContext.getSink(path="s3://aa-stedi-prjct/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1748093491483")
AmazonS3_node1748093491483.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1748093491483.setFormat("json")
AmazonS3_node1748093491483.writeFrame(SQLQuery_node1748093022155)
job.commit()