from spark_process_common.validation_runner import ValidationRunner

if __name__ == "__main__":
    SPARK_CONFIG = {
        "spark.sql.hive.convertMetastoreOrc": "true",
        "spark.sql.files.ignoreMissingFiles": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.hive.verifyPartitionPath": "false",
        "spark.sql.orc.filterPushdown": "true",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "hive.exec.dynamic.partition.mode": "nonstrict",
        "hive.exec.dynamic.partition": "true",
        "spark.sql.execution.arrow.enabled": "true"
    }
    validator = ValidationRunner(spark_config=SPARK_CONFIG)
    validator.execute()
