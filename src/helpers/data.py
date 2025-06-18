import requests
from typing import Optional, List, Dict
from pyspark.sql import DataFrame as SparkDataFrame

from src.services.spark import Spark

class Data:

    @classmethod
    def _read_csv_spark(cls, rel_path:str, **options) ->  SparkDataFrame:
        return cls.spark.read.option("header", "true").option("inferSchema", "true").csv(rel_path)
   
    @classmethod
    def _read_parquet_spark(cls, rel_path:str, **options) ->  SparkDataFrame:
        return cls.spark.read.parquet(rel_path)
    
    @classmethod
    def api(cls, url, **options) -> List[Dict]:
        return requests.get(url).json()

    @classmethod
    def read(cls):
        if file_type == FileType.CSV:
                return cls._read_csv_pandas(rel_path, **options)

        if file_type == FileType.PARQUET:
            return cls._read_parquet_pandas(rel_path, **options)
