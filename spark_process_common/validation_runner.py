from importlib import import_module
from pprint import pformat
from typing import Dict, List

from spark_process_common.spark_job import BaseSparkJob


class ValidationException(Exception):
    pass


class ValidationRunnerException(Exception):
    pass


class ValidationRunner(BaseSparkJob):
    """
    A "Runner" class for pipeline validation tasks that can easily be configured by a single job config object.
    This runner class iterates through a list of `validators` defined in the config object and executes a list of
    `steps`.

    Validators:
        The given configuration object should contain a list of `validators`. A `validator` is a self contained
        dict/json object that defines the step/steps to run.

    Validation Steps:
        If the step passes validation, it should return a value that will evaluate to True in an any/all function.
        If the step fails validation, it should return a value that will evaluate to False in an any/all function.

    class_kwargs and default_kwargs currently supported: config, spark, fiscal_year, month_abbr, sources, logger

    Example configuration - This configuration will run 4 steps to validate that 4 source files contain records.
    If any step returns an invalid result, the process will continue until all steps are complete:

        {
          "app_name": "validate_sources",
          "fail_fast": false,                       -- Fail immediately or run all validation steps
          "validators": [                           -- List of validators
            {
              "module": "spark_process_common.source_record_counter", -- The module to load
              "class": "SourceRecordCounter",                         -- The class to load from the module
              "class_kwargs": [                                       -- The keyword arguments to instantiate the class
                "spark", "logger"
              ],
              "steps": [
                {
                  "name": "validate_edw_sales_detail",               -- The step's key in the final result dict
                  "method": "get_record_count_for_period",           -- The first method to call
                  "default_kwargs": [                                -- The keyword arguments to pass into the method
                    "sources", "fiscal_year", "month_abbr"
                  ],
                  "extra_kwargs": {                       -- Additional/custom keyword arguments to pass into the method
                    "source_name": "edw_sales_detail"
                  }
                },
                {
                  "method": "get_record_count",                      -- The second method/step to call
                  "name": "validate_edw_location",
                  "default_kwargs": ["sources"],
                  "extra_kwargs": {"source_name": "edw_location"}
                },
                {
                  "method": "get_record_count",                      -- The third method/step to call
                  "name": "validate_edw_customer_location",
                  "default_kwargs": ["sources"],
                  "extra_kwargs": {"source_name": "edw_customer_location"}
                },
                {
                  "method": "get_record_count",                     -- The fourth method/step to call
                  "name": "validate_edw_parent_customer"
                  "default_kwargs": ["sources"],
                  "extra_kwargs": {"source_name": "edw_parent_customer"}
                }
              ],

            },
          "sources": {
              "edw_sales_detail": {
              "path": "s3://wrktdtransformationrawproddtl001/enterprise-sales/edw/SalesDetails/*.csv",
              "date_format": "dd-MMM-yy",
              "primary_filter_column": "ACCOUNTING_PERIOD",
              "secondary_filter_column": "ACCOUNTING_PERIOD"
            },
            "edw_location": {
              "path": "s3://wrktdtransformationrawproddtl001/enterprise-sales/edw/Location/EDW_location.csv",
              "format": "csv"
            },
            "edw_customer_location": {
              "path": "s3://wrktdtransformationrawproddtl001/enterprise-sales/edw/CustomerLocation/EDW_customerLocation.csv",
              "format": "csv"
            },
            "edw_parent_customer": {
              "path": "s3://wrktdtransformationrawproddtl001/enterprise-sales/edw/ParentCustomer/EDW_parentCustomer.csv",
              "format": "csv"
            },
          }
        }

    ** Dependencies:

    If the validator class/module is not already in the the sys path, you must define the dependency in the
    `validator`.
        IE: "dependency": "s3://wrktdtransformationprocessproddtl001/ent-sales/process/1.1/my_file.py"

    """

    def get_dependencies(self) -> List:
        """
        Overwritten to parse dependencies from validators in the config object.

        {
          "module": "my_module",
          "class": "MyClass",
          "class_kwargs": ["spark", "config"],
          "steps": [
            {
              "method": "my_method",
              "default_kwargs": ["spark", "config", "fiscal_year", "month_abbr"]
            }
          ],
          "dependency": "s3://wrktdtransformationprocessproddtl001/ent-sales/process/1.1/my_module.py"
        }
        """
        validators = self.get_config()['validators']
        dependencies = [v.get('dependency') for v in validators if v.get('dependency')]
        return dependencies

    def add_arguments(self, parser):
        parser.add_argument('fiscal_year', help='Fiscal Year to process, used for filtering data.', type=str)
        parser.add_argument('month_abbr', help='Month within the given Fiscal Year to process.', type=str)

    def execute(self):
        """
        Iterate through all of the validators in the config and call their respective validation methods.

        This process will raise a ValidationException (to cascade fail downstream data pipeline activities) if a any
        validation job returns a value that evaluates to False in an any() statement (None, 0, '', [], {}, etc.).
        """
        config = self.get_config()
        validators = config.get('validators', [])
        fail_fast = config.get('fail_fast', False)
        kwarg_opts = self.build_optional_kwarg_mapping()
        results = {}

        for validator in validators:
            validation_result = self.run_validator(validator, kwarg_opts)
            results.update(validation_result)
            if fail_fast and not self.all_results_pass(results):
                msg = f"""
                A validation job has failed with fail fast set to True. Terminating... 
                Results: {pformat(results)}
                """
                self.logger.error(msg)
                raise ValidationException(msg)
        if results:
            self.process_results(results)

    def build_optional_kwarg_mapping(self) -> Dict:
        config = self.get_config()
        return {
            'config': config,
            'spark': self.get_spark(),
            'fiscal_year': config.get('fiscal_year'),
            'month_abbr': config.get('month_abbr', '').upper(),
            'sources': config.get('sources', {}),
            'logger': self.logger
        }

    @staticmethod
    def all_results_pass(results: dict) -> bool:
        return all(results.values())

    def get_validator_obj(self, validator_config: Dict, kwarg_opts: Dict) -> Dict:
        _class_name = validator_config.get('class')
        init_kwargs = {key: kwarg_opts[key] for key in validator_config.get('class_kwargs', [])}
        mod = self.import_validator_module(validator_config)
        # Allows for functions to execute if no class is defined in the config
        return getattr(mod, _class_name)(**init_kwargs) if _class_name else mod

    @staticmethod
    def import_validator_module(validator_config):
        return import_module(validator_config['module'])

    def process_results(self, results: Dict):
        if self.all_results_pass(results):
            self.logger.info(f"Validation runner complete! Results: \n {pformat(results)}")
        else:
            msg = f"One or more jobs have failed validation. Results: \n {pformat(results)}"
            self.logger.error(msg)
            raise ValidationException(msg)

    def run_validator(self, validator_config: Dict, kwarg_opts: Dict) -> Dict:
        validator = self.get_validator_obj(validator_config, kwarg_opts)
        return {
            f"{step['name']}": self.run_validation_method(validator, step, kwarg_opts)
            for step in validator_config['steps']
        }

    @staticmethod
    def run_validation_method(obj: object, config: Dict, kwarg_opts: Dict) -> Dict:
        method_kwargs = {key: kwarg_opts[key] for key in config.get('default_kwargs', [])}
        extra_kwargs = config.get('extra_kwargs', {})
        if extra_kwargs:
            method_kwargs.update(extra_kwargs)
        return getattr(obj, config['method'])(**method_kwargs)
