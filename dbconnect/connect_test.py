from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.serverless().profile("dbc-9c7dbe12-0a2f").getOrCreate()

df = spark.read.table("samples.nyctaxi.trips")
df.show(5)