from pathlib import Path
import sys
sys.path.append(str(Path("C:/Users/RAVICRA2/OneDrive - Alcon/adl-customer-mdm-etl")))
from src.helpers.dataclass import Dnb
from src.helpers.data import Data

try:
    dict(Dnb)
    # get metadata
except Exception:
    pass
else:
    pass
finally:
    pass

# from pyspark.sql import SparkSession
# from pyspark.sql import Row


# # Initialize SparkSession
# spark = SparkSession.builder.appName("SparkDataframe").getOrCreate()
# spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
# # Execute SQL query and create a DataFrame
# df=spark.sql("select * from adl_enriched_gbl_cf_ebx.ebx_hierarchy_hierarchy_dunAndBradstreetAccounts")
# # S3 bucket path
# s3_bucket="s3://adl-base-customer-mdm-etl-dev-226aog/raw/FS_DNB/"
# # Write DataFrame to S3 in Parquet format

# df.write.format("parquet").mode("overwrite").option("encoding", "UTF-8").save(s3_bucket)

# print("Data successfully written")

# # Get Environemnt

# # source, layer, system, entity