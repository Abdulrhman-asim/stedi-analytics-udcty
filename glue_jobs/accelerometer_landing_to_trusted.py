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
customer_trusted_node1748019862541 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1748019862541")

# Script generated for node accelerometer_landing
accelerometer_landing_node1748019884944 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://aa-stedi-prjct/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1748019884944")

# Script generated for node SQL Query
SqlQuery547 = '''
select acc.user, x, y, z, timestamp
FROM accelerometer_landing acc
    JOIN customer_trusted ct ON acc.user = ct.email
    WHERE ct.sharewithresearchasofdate < acc.timestamp
'''
SQLQuery_node1748020560341 = sparkSqlQuery(glueContext, query = SqlQuery547, mapping = {"accelerometer_landing":accelerometer_landing_node1748019884944, "customer_trusted":customer_trusted_node1748019862541}, transformation_ctx = "SQLQuery_node1748020560341")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1748020560341, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1748019354255", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1748021631309 = glueContext.getSink(path="s3://aa-stedi-prjct/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1748021631309")
AmazonS3_node1748021631309.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1748021631309.setFormat("json")
AmazonS3_node1748021631309.writeFrame(SQLQuery_node1748020560341)
job.commit()