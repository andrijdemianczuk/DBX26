from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

#Initialize the spark and workspace contexts.
spark = DatabricksSession.builder.serverless().profile("dbc-9c7dbe12-0a2f").getOrCreate()
w = WorkspaceClient(profile = "dbc-9c7dbe12-0a2f")

#Test a connection to a known delta table and work with remote dataframes
df = spark.read.table("samples.nyctaxi.trips")
df.show(5)

#Test the dbutils capabilities
list = w.dbutils.fs.ls("/")
for file in list:
    print(file)

file_path = "/Volumes/main/default/my-volume/zzz_hello.txt"
file_data = "Hello, Databricks!"
fs = w.dbutils.fs

fs.put(
  file      = file_path,
  contents  = file_data,
  overwrite = True
)

print(fs.head(file_path))

fs.rm(file_path)