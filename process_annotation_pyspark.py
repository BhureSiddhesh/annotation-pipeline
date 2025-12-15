from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
from pyspark.sql.functions import col, collect_set, size, current_timestamp, concat_ws
import os

def process_batch(batch_df, batch_id): 
    batch_df = batch_df.filter(col("confidence_score").cast("double") >= 0.8)
    disagreement_df = (
        batch_df.groupBy("text")
                .agg(collect_set("label").alias("distinct_labels"))
                .withColumn("num_distinct_labels", size(col("distinct_labels")))
                .filter(col("num_distinct_labels") > 1)
    )
    # Example: show or write to sink
    disagreement_df.show(truncate=False)

    non_disagree_df = batch_df.join(
    disagreement_df.select("text"),
    on="text",
    how="left_anti"
    )

    rows = disagreement_df.collect()
    with open("output\disagreement\disagreements.log", "a", encoding="utf-8") as f:
        for r in rows:
            # r.distinct_labels is a list; join into comma-separated string
            labels = ",".join(r["distinct_labels"]) if r["distinct_labels"] else ""
            line = f'{r["text"]} | {labels} | {r["num_distinct_labels"]}\n'
            f.write(line)

    non_disagree_df.show(truncate=False)

    # output_dir = "clean_training_dataset.json"  # adjust path as needed
    # non_disagree_df.write.mode("append").json(output_dir, lineSep="\n")

    json_lines = non_disagree_df.toJSON().collect()

    with open("clean_training_dataset.jsonl", "a", encoding="utf-8") as f:
        for line in json_lines:
            f.write(line.rstrip("\n") + "\n")


print("Starting the Spark Context and Streaming Context...")
spark = SparkSession.builder.appName("AnnotationProcessor").master("local[*]").getOrCreate()

input_schema = StructType([
    StructField("text", StringType(), True),
    StructField("annotator_id", IntegerType(), True),
    StructField("label", StringType(), True),
    StructField("confidence_score", DoubleType(), True)
])

raw = spark.readStream.schema(input_schema).format("csv").option("header", "true").load("CSV")

stream = raw.withColumn("ingest_timestamp", current_timestamp())
query = (
    stream.writeStream
          .foreachBatch(process_batch)
          .start()
)


query.awaitTermination()
