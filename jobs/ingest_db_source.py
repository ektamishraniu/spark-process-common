"""
Ingest job flow for Database based Data sources.
"""
from spark_process_common.ingest import IngestDbSource


if __name__ == '__main__':
    spark_config = {
        "spark.sql.hive.convertMetastoreOrc": "true",
        "spark.sql.files.ignoreMissingFiles": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.hive.verifyPartitionPath": "false",
        "spark.sql.orc.filterPushdown": "true",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "hive.exec.dynamic.partition.mode": "nonstrict",
        "hive.exec.dynamic.partition": "true"         
    }

    # Ingest 
    with IngestDbSource(spark_config=spark_config) as ingest:
        ingest.execute()

    """
    File (Example):
    "source_path": "s3://wrktdtransformationrawproddtl001/mill-profitability/ppmrps/ingest/mstr_material_vw/"
    """
