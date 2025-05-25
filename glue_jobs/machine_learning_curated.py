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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1748131764783 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1748131764783")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1748131737843 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_trainer_trusted_node1748131737843")

# Script generated for node SQL Query
SqlQuery506 = '''
select * 
from 
    accelerometer_trusted acc_t join step_trainer_trusted step_t on acc_t.timestamp = step_t.sensorreadingtime
'''
SQLQuery_node1748131957978 = sparkSqlQuery(glueContext, query = SqlQuery506, mapping = {"accelerometer_trusted":accelerometer_trusted_node1748131764783, "step_trainer_trusted":step_trainer_trusted_node1748131737843}, transformation_ctx = "SQLQuery_node1748131957978")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1748131957978, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1748127165249", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1748132308571 = glueContext.getSink(path="s3://aa-stedi-prjct/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1748132308571")
machine_learning_curated_node1748132308571.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machine_learning_curated_node1748132308571.setFormat("json")
machine_learning_curated_node1748132308571.writeFrame(SQLQuery_node1748131957978)
job.commit()