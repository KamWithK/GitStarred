#!/usr/bin/env python

from ProcessTrainPipeline import ProcessTrainPipeline
from PipelineComponents import RemoveColumns, CastToInt, CleanUp
from pyspark.ml.feature import MinMaxScaler, VectorAssembler

pipeline = ProcessTrainPipeline("jdbc:postgresql:GitHubData", "program", "DatabaseAccess", load_percent=1)

# Remove alternative popularity metrics (which aren't used)
drop = RemoveColumns("id", "forkcount", "watchs", "users", "issues", "pullrequests")

# Convert boolean values into integers (0's and 1's) to avoid problems when testing (caused by type casting)
cast_to_int = CastToInt("fork", "locked", "archived")

# Min-Max feature scaling
assembler = VectorAssembler(inputCols=["stars"], outputCol="starsVector")
scaler = MinMaxScaler(inputCol="starsVector", outputCol="target")

# Temporarily remove all text features (until they're used) and clean vector column
drop.columns = [*drop.columns, "name", "description", "topics", "readme"]
cleanup = CleanUp("target")

# Run the full pipeline for data processing and modelling training
pipeline.process_data(drop, assembler, scaler, cast_to_int, cleanup)
model, results = pipeline.train_models(max_runtime=120)
pipeline.visualise_model(results)
