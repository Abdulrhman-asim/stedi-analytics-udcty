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

# Script generated for node customer_curated
customer_curated_node1748129795324 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customer_curated_node1748129795324")

# Script generated for node step_trainer_landing
step_trainer_landing_node1748129891375 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://aa-stedi-prjct/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1748129891375")

# Script generated for node SQL Query
SqlQuery566 = '''
select step_land.sensorreadingtime, step_land.serialnumber, step_land.distancefromobject
from 
    step_trainer_landing step_land JOIN customer_curated cust_cur on cust_cur.serialnumber = step_land.serialnumber
'''
SQLQuery_node1748130364595 = sparkSqlQuery(glueContext, query = SqlQuery566, mapping = {"step_trainer_landing":step_trainer_landing_node1748129891375, "customer_curated":customer_curated_node1748129795324}, transformation_ctx = "SQLQuery_node1748130364595")

# Script generated for node step_trainer_trusted_target
EvaluateDataQuality().process_rows(frame=SQLQuery_node1748130364595, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1748127165249", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_target_node1748131422791 = glueContext.getSink(path="s3://aa-stedi-prjct/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_target_node1748131422791")
step_trainer_trusted_target_node1748131422791.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_target_node1748131422791.setFormat("json")
step_trainer_trusted_target_node1748131422791.writeFrame(SQLQuery_node1748130364595)
job.commit()