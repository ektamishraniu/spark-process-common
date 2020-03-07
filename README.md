Support library for Spark Processing Scripts
=======================

Installing for Development
---

### Clone the Repo

From the command line pull down the latest code:

```
git clone ...../spark_process_common
```

Always create a virtual environment for your Python Apps!!!

### Create a virtual environment 

Use the following command to create a virtual environment for the app:

```
python3.6 -m venv venv36
```

### Activate the virtual environment

Use the following command to activate the virtual environment so you can begin installing dependencies.

```
source venv36/bin/activate
```

Note: Once the virtual environment is activated, the command prompt should change to show you are in the venv36.

### Connecting to Artifactory for Private Pypi

In order for private Pypi packages to be downloaded (this includes things like spark-process-common) and additional index-url needs to be added to the list for pip.  You will need to edit your ~/.pip/pip.conf file to include the following content:

```
[global]
extra-index-url = https://td-transformation:<password>@wrktdartifactory.jfrog.io/wrktdartifactory/api/pypi/pypi-local/simple
```

Note: The td-transformation user described above has been granted read access to the private Pypi repository.  The Password has been omitted from this example, see the DevOps team for the password.

### Install the python package and it's dependencies in Development Mode

```
pip3.6 install -e .[dev,test]
```

Packaging for Spark Environment
---

The spark_process_common library contains a bunch of utilities necessary for spark jobs to run.  This library needs to be included in the environment where the Spark job is executing.  The first step that needs to be completed is packaging for use in Spark.  The Spark clusters used by Technology Development require dependencies to be included in egg files.

### Creating an egg file

Use the following command to create the egg file.  This command will generate an egg file based on the python version you are using.  Make sure you are in your Virtual Environment when you run the following command.

```
python setup.py bdist_egg
```

Spark Notes and Best Practices
---

### The SparkSession, SQLContext, and HiveContext

In previous versions of Spark, the SQLContext and HiveContext provided the ability to work with DataFrames and Spark SQL and were commonly stored as the variable sqlContext in examples, documentation, and legacy code. As a historical point, Spark 1.X had effectively two contexts. The SparkContext and the SQLContext. These two each performed different things. The former focused on more fine-grained control of Spark’s central abstractions, whereas the latter focused on the higher-level tools like Spark SQL. In Spark 2.X, the communtiy combined the two APIs into the centralized SparkSession that we have today. However, both of these APIs still exist and you can access them via the SparkSession. It is important to note that you should never need to use the SQLContext and rarely need to use the SparkContext.

## General Structure for Writing Spark Jobs

Prefer dependency injection patterns when possible, and try to avoid hard-coding things like data source paths, service locations, and db connection strings. Instead, data source paths should be passed as arguments to `spark-submit`.

Write simple functions / methods for extracting source data files. For consistency throughout our codebase, the current convention is these functions should use `extract_` as a prefix, and accept args/kwargs for any parameters required to do high-level filtering or simple transformations that should apply to the entire dataframe.

```
    def extract_otm(self, path: str, year: str, month: str) -> DataFrame:
        otm_merged = (
            self.spark.read.csv(path, header=True, inferSchema=True)
            .withColumnRenamed('FISCALYEAR', 'fiscal_year')
            .withColumnRenamed('PERIOD', 'month_abbr')
            .where(F.col('fiscal_year') == year)
            .where(F.col('month_abbr') == month)
        )
        return otm_merged
```

The `spark_process_common.models.BaseTransform` base class provides a `transform()` method, which should be used similarly to a `main()` function. All of the smaller, more modular transformations will be encapsulated within `.transform()`.

Any parameters that need to be passed in via `spark-submit` should be defined in the `if __name__ == '__main__':` block. This is also where we create the `SparkSession`, and use it to instantiate transformation classes.

```
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'otm_merged_source',
        help='S3 path for OTM Merged Data Excl Non-CB Cost Centers Data.',
        type=str)
    parser.add_argument(
        'fiscal_year',
        help='Fiscal Year to process, used for filtering data.',
        type=str)
    parser.add_argument(
        'month_abbr',
        help='Month (abbreviated) within the given Fiscal Year to process.',
        type=str)
    parser.add_argument(
        '--output_bucket',
        help='Destination S3 bucket where transformed data files will be stored.',
        default='transformations',
        type=str)
    parser.add_argument(
        '--output_prefix',
        help='Prefix used when storing new output files in the destination S3 bucket.',
        default='non_tms_freight',
        type=str)
    args = parser.parse_args()

    spark = SparkSession.builder.appName('Non TMS Freight Job Flow').getOrCreate()

    NonTmsFreight(spark).transform(**vars(args))

    spark.stop()

```

Break down the entire job into modular transformation methods that can be tested in isolation. Ideally each of these methods would accept dataframes as inputs and return the joined / merged / transformed dataframe. In most situations these will be static methods, but the `SparkSession` can be accessed via `self.spark` where needed. Again for the sake of consistency, the current convention is these functions / methods should use `transform_` as a prefix.

```
    @staticmethod
    def transform_summary_jde(jde_df: DataFrame) -> DataFrame:
        otm_filter = F.col('Category') == 'OTM/ADV/MQM'
        tms_filter = F.col('Category') == 'TMS X1 Accrual'
        jde_otm_summary_df = (
            jde_df.where((otm_filter | tms_filter))
            .groupBy('Mill', 'BU #', 'Category', 'fiscal_year', 'month_abbr')
            .agg(F.sum('Amount').alias('SumOfAmount'))
        )
        return jde_otm_summary_df
```

Using the structure outlined above, we can write unit tests for each of the individual transformation steps that can be run in isolation, and even concurrently. Define pytest fixtures at the module level for dataframes and/or resources that are used in multiple tests to avoid the overhead of reading source files multiple times. In the near future, we will likely build some additional testing utilities that leverage temp tables and broadcast variables stored at the SparkSesssion level to make testing even easier.

