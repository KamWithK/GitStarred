#!/usr/bin/env python

import h2o
import sys, os

import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pysparkling import H2OContext
from pysparkling.ml import H2OAutoML

# Function to avoid uneccessary outputs being printed to terminal
def deafen(function, *args):
    real_stdout = sys.stdout
    sys.stdout = open(os.devnull, "w")
    output = function(*args)
    sys.stdout = real_stdout
    return output

class ProcessTrainPipeline():
    def __init__(self, connection_url, username, password, driver_class="org.postgresql.Driver", load_percent=10, seed=1, show=False):
        spark = SparkSession.builder.appName("GitStarred") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.14") \
            .config("spark.sql.autoBroadcastJoinThreshold", -1) \
            .config("spark.executor.memory", "13g").config("spark.driver.memory", "13g") \
            .getOrCreate()

        self.seed = seed

        # Importing small subset of data
        if load_percent != None and load_percent < 100:
            query = f"(SELECT * FROM Repositories TABLESAMPLE SYSTEM({load_percent}) REPEATABLE({self.seed})) AS temp"
            self.raw_data = spark.read.jdbc(connection_url, query, properties={"user": username, "password": password, "driver": driver_class})
        else: self.raw_data = spark.read.jdbc(connection_url, "Repositories", properties={"user": username, "password": password, "driver": driver_class})

        # Show input data
        if show == True:
            self.raw_data.printSchema()
            print(f"{self.raw_data.count()} rows")

    # Create and run pipeline
    def process_data(self, *stages, split=[0.8, 0.2], show=True):
        pipeline = Pipeline(stages=stages)
        data = pipeline.fit(self.raw_data).transform(self.raw_data)
        if show == True: data.show()

        self.train_data, self.test_data = data.randomSplit(split, seed=self.seed)

    # Start H2O, train models and show results
    # Note that data processing must be run beforehand
    def train_models(self, max_runtime=None, max_models=0):
        H2OContext.getOrCreate()

        self.automl = H2OAutoML(maxRuntimeSecs=max_runtime, maxModels=max_models, labelCol="target", convertUnknownCategoricalLevelsToNa=True, convertInvalidNumbersToNa=True, seed=self.seed)
        model = self.automl.fit(self.train_data)
        results = model.transform(self.test_data)

        return model, results

    def visualise_model(self, results):
        results.show()
        self.automl.getLeaderboard("ALL").show(truncate=False)

        # Display a variable importance plot for the best supporting model
        def var_imp(model_id):
            varimp = deafen(h2o.get_model(model_id).varimp)
            if varimp != None: print(varimp)
            else: return False

        for model in self.automl.getLeaderboard("ALL").collect():
            if var_imp(model["model_id"]) != False: break
