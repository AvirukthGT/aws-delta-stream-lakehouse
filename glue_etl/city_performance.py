import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
import gs_derived
import re
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
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

# Script generated for node Amazon S3
AmazonS3_node1770902154719 = glueContext.create_dynamic_frame.from_catalog(database="ecommerce_silver_db", table_name="fact_sales_enriched", transformation_ctx="AmazonS3_node1770902154719")

# Script generated for node Filter Standard
FilterStandard_node1770902425405 = Filter.apply(frame=AmazonS3_node1770902154719, f=lambda row: (row["total_amount"] < 500), transformation_ctx="FilterStandard_node1770902425405")

# Script generated for node Filter Premium
FilterPremium_node1770902411499 = Filter.apply(frame=AmazonS3_node1770902154719, f=lambda row: (row["total_amount"] >= 500), transformation_ctx="FilterPremium_node1770902411499")

# Script generated for node Agg Standard
AggStandard_node1770902585673 = sparkAggregate(glueContext, parentFrame = FilterStandard_node1770902425405, groups = ["city"], aggs = [["total_amount", "sum"]], transformation_ctx = "AggStandard_node1770902585673")

# Script generated for node Agg Premium
AggPremium_node1770902488549 = sparkAggregate(glueContext, parentFrame = FilterPremium_node1770902411499, groups = ["city"], aggs = [["total_amount", "sum"]], transformation_ctx = "AggPremium_node1770902488549")

# Script generated for node Rename Field
RenameField_node1770902657889 = RenameField.apply(frame=AggStandard_node1770902585673, old_name="`sum(total_amount)`", new_name="standard_revenue", transformation_ctx="RenameField_node1770902657889")

# Script generated for node Rename Field
RenameField_node1770902639041 = RenameField.apply(frame=AggPremium_node1770902488549, old_name="`sum(total_amount)`", new_name="premium_revenue", transformation_ctx="RenameField_node1770902639041")

# Script generated for node Join Streams
JoinStreams_node1770902693925 = Join.apply(frame1=RenameField_node1770902639041, frame2=RenameField_node1770902657889, keys1=["city"], keys2=["city"], transformation_ctx="JoinStreams_node1770902693925")

# Script generated for node Join Streams
JoinStreams_node1770902798217 = JoinStreams_node1770902693925.gs_derived(colName="Calc Ratios", expr="(premium_revenue / (premium_revenue + standard_revenue)) * 100")

# Script generated for node Select Fields
SelectFields_node1770902984987 = SelectFields.apply(frame=JoinStreams_node1770902798217, paths=["city", "standard_revenue", "premium_revenue", "Calc Ratios"], transformation_ctx="SelectFields_node1770902984987")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SelectFields_node1770902984987, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1770902140632", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1770903097419 = glueContext.getSink(path="s3://event-driven-lakehouse-curated-b8ad22ee/city_performance_matrix/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1770903097419")
AmazonS3_node1770903097419.setCatalogInfo(catalogDatabase="database_tuts",catalogTableName="gold_city_matrix")
AmazonS3_node1770903097419.setFormat("glueparquet", compression="snappy")
AmazonS3_node1770903097419.writeFrame(SelectFields_node1770902984987)
job.commit()