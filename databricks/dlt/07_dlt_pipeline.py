import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name="stage_bookings"
)
def stage_bookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronzes/bronzevolume/bookings/data")
    return df

@dlt.table(
    name="trans_bookings"
)
def trans_bookings():
    df = dlt.read_stream("stage_bookings")
    df = df.withColumn("amount", col("amount").cast(DoubleType()))\
           .withColumn("updated_timestamp", current_timestamp())\
           .withColumn("booking_date", to_date(col("booking_date")))\
           .drop("_rescued_data")
    return df

rules = {
    "rule1": "booking_id is not null",
    "rule2": "passenger_id is not null"
}

@dlt.table(
    name="silver_bookings"
)
@dlt.expect_all(rules)
def silver_bookings():
    df = dlt.read_stream("trans_bookings")
    return df


###Flights

@dlt.view(
    name="trans_flights"
)
def trans_flights():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronzes/bronzevolume/flights/data/")
    df = df.withColumn("flight_date", to_date(col("flight_date"), "yyyy-MM-dd"))\
           .withColumn("modified_date", current_timestamp())\
           .drop("_rescued_data")
    return df

dlt.create_streaming_live_table("silver_flights")

dlt.apply_changes(
    target="silver_flights",
    source="trans_flights",
    keys=["flight_id"],
    sequence_by=col("modified_date"),
    stored_as_scd_type=1
)

###Passengers

@dlt.view(
    name="trans_passengers"
)

def trans_passengers():
   

    df=spark.readStream.format("delta").load("/Volumes/workspace/bronzes/bronzevolume/customers/data")\
    .withColumn("modified_date",current_timestamp())\
    .drop("_rescued_data")
    return df

dlt.create_streaming_live_table("silver_passengers")

dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="trans_passengers",
    keys=["passenger_id"],
    sequence_by=col("modified_date"),
    stored_as_scd_type=1
)

###airports data

@dlt.view(
    name="trans_airports"
)
def trans_airports():
    df=spark.readStream.format("delta").load("/Volumes/workspace/bronzes/bronzevolume/airports/data/")\
    .withColumn("modified_date",current_timestamp())\
        .drop("_rescued_data")
    return df

dlt.create_streaming_live_table("silver_airports")

dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="trans_airports",
    keys=['airport_id'],
    sequence_by=col('modified_date'),
    stored_as_scd_type=1
)


###silver business view
@dlt.table(name="silver_business")
def silver_business():
    df = dlt.read("silver_bookings")\
            .join(dlt.read("silver_flights"), ["flight_id"])\
            .join(dlt.read("silver_passengers"), ["passenger_id"])\
            .join(dlt.read("silver_airports"), ["airport_id"])\
            .drop("modified_date")
    return df