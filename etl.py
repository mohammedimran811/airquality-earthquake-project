import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import coalesce
from pyspark.sql.types import DoubleType, StructType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = "saqcp-data-lake-demo2026"
CURATED_PATH = f"s3://{BUCKET}/curated/environmental_events/"

# --- Load raw earthquake data directly from S3 (bypasses the Glue Catalog
# entirely for this job, avoiding crawler schema-naming inconsistencies) ---
eq_df = spark.read.option("multiLine", "true").option("recursiveFileLookup", "true").json(f"s3://{BUCKET}/raw/earthquakes/")

# USGS coordinates arrays sometimes have mixed int/double sub-elements
# (depth is often a whole number while lon/lat have decimals), which Spark's
# schema inference can merge into a struct instead of a plain double. Rather
# than guess the sub-field names, inspect the real inferred schema at run
# time and build the right expression for whatever shape actually exists.
# "geometry" lives inside each element of the "features" array, not at the
# top level of the document, so drill into features -> element -> geometry.
features_element_type = eq_df.schema["features"].dataType.elementType
coords_element_type = (
    features_element_type["geometry"].dataType["coordinates"].dataType.elementType
)

if isinstance(coords_element_type, StructType):
    sub_field_names = [f.name for f in coords_element_type.fields]

    def extract_coord(col_expr):
        return coalesce(*[col_expr.getField(name).cast(DoubleType()) for name in sub_field_names])
else:
    def extract_coord(col_expr):
        return col_expr.cast(DoubleType())

# The USGS GeoJSON nests fields under "features" -> "properties" / "geometry"
eq_flat = eq_df.select(F.explode("features").alias("f")).select(
    F.col("f.properties.mag").alias("magnitude"),
    F.col("f.properties.place").alias("place"),
    (F.col("f.properties.time") / 1000).cast("timestamp").alias("event_time"),
    extract_coord(F.col("f.geometry.coordinates")[0]).alias("eq_lon"),
    extract_coord(F.col("f.geometry.coordinates")[1]).alias("eq_lat"),
)

# --- Load raw air quality data directly from S3 ---
# Each file's root is a JSON array of {location_id, data} objects. The
# multiLine option tells Spark to parse the whole file as one JSON document
# and, since the root is an array, each array element becomes its own row
# with location_id and data as plain top-level columns -- no crawler-specific
# "array" wrapper column to work around.
aq_df = spark.read.option("multiLine", "true").option("recursiveFileLookup", "true").json(f"s3://{BUCKET}/raw/air_quality/")

aq_flat = aq_df.select(
    "location_id",
    F.explode("data.results").alias("r"),
).select(
    "location_id",
    F.col("r.sensorsId").alias("sensor_id"),
    F.col("r.value").alias("aqi_value"),
    F.col("r.coordinates.latitude").alias("aq_lat"),
    F.col("r.coordinates.longitude").alias("aq_lon"),
    F.to_timestamp(F.col("r.datetime.utc")).alias("reading_time"),
)

# --- Haversine distance in km between earthquake and AQ sensor ---
def haversine_expr(lat1, lon1, lat2, lon2):
    r = 6371.0
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    c = 2 * F.asin(F.sqrt(a))
    return r * c

joined = eq_flat.crossJoin(aq_flat).withColumn(
    "distance_km",
    haversine_expr(F.col("eq_lat"), F.col("eq_lon"), F.col("aq_lat"), F.col("aq_lon")),
).filter(
    (F.col("distance_km") <= 100) &
    (F.abs(F.col("event_time").cast("long") - F.col("reading_time").cast("long")) <= 3600 * 6)
)

curated = joined.select(
    "magnitude", "place", "event_time", "eq_lat", "eq_lon",
    "location_id", "sensor_id", "aqi_value", "reading_time",
    "distance_km",
).withColumn("event_date", F.to_date("event_time"))

curated.write.mode("append").partitionBy("event_date").parquet(CURATED_PATH)

job.commit()
