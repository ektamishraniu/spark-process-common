"""
Common job config code

To retrieve json from an s3 bucket/path
To update json in an s3 bucket/path
"""
import boto3
import json

from datetime import datetime


class JobConfig:

    def __init__(self, config_bucket: str, config_path: str):
        s3 = boto3.resource('s3')
        self.__config_object = s3.Object(config_bucket, config_path)

    def get(self) -> dict:
        """
        Returns job configuration from JSON
        """
        config_content = self.__config_object.get()['Body'].read().decode('utf-8')
        config = json.loads(config_content)

        return config

    def update(self, config: dict):
        """
        Allows for the addition, update, or removal of JSON configution flexibly

        ** Consider for removal once additional methods for config manipulation have been identified.
        """
        self.__config_object.put(
            Body=json.dumps(
                config,
                ensure_ascii=False,
                indent=4
            )
        )

    def update_last_run_dttm(self):
        """
        Adds / updates the last_run_dttm value in the config file
        """
        last_run_dttm = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        config = self.get()
        config['last_run_dttm'] = last_run_dttm
        self.update(config)
