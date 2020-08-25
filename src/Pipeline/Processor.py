#!/usr/bin/env python

import h2o

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from petastorm.unischema import Unischema
from petastorm.etl.dataset_metadata import materialize_dataset

class Processor():
    def __init__(self, connection_url, username, password, driver_class="org.postgresql.Driver", load_percent=10, seed=1, show=False):
        self.spark = SparkSession.builder.appName("GitStarred") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.14") \
            .config("spark.sql.autoBroadcastJoinThreshold", -1) \
            .config("spark.executor.memory", "10g").config("spark.driver.memory", "10g") \
            .getOrCreate()

        self.seed = seed

        # Importing small subset of data
        if load_percent != None and load_percent < 100:
            query = f"(SELECT * FROM Repositories TABLESAMPLE SYSTEM({load_percent}) REPEATABLE({self.seed})) AS temp"
            self.raw_data = self.spark.read.option("fetchsize", "10000").jdbc(connection_url, query, numPartitions=10000, properties={"user": username, "password": password, "driver": driver_class})
        else: self.raw_data = self.spark.read.option("fetchsize", "10000").jdbc(connection_url, "Repositories", numPartitions=10000, properties={"user": username, "password": password, "driver": driver_class})

        # Show input data
        if show == True:
            self.raw_data.printSchema()
            print(f"{self.raw_data.count()} rows")

    # Create and run pipeline
    def process_data(self, *stages, split=[0.8, 0.2], show=True):
        pipeline = Pipeline(stages=stages)
        data = pipeline.fit(self.raw_data).transform(self.raw_data)
        if show == True: data.dropna().show()

        self.train_data, self.test_data = data.randomSplit(split, seed=self.seed)

    def save_processed_data(self, train_path, test_path, schema: Unischema):
        # Ensure data folder exists
        train_path.parent.mkdir(parents=True, exist_ok=True)

        # Save data and metadata
        with materialize_dataset(self.spark, train_path.absolute().as_uri(), schema):
            self.train_data.write.option("maxRecordsPerFile", 3000).mode("overwrite").parquet(str(train_path))

        with materialize_dataset(self.spark, test_path.absolute().as_uri(), schema):
            self.test_data.write.option("maxRecordsPerFile", 3000).mode("overwrite").parquet(str(test_path))

        print("Saved all processed data")
