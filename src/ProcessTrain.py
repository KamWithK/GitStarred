#!/usr/bin/env python

from pathlib import Path
from Pipeline.Processor import Processor
from Modelling.H2OSparkAutoML import H2OSparkAutoML
from Pipeline.Components import RemoveColumns, CastToInt, ExtractTextFeatures, CleanUp
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from petastorm.unischema import Unischema, UnischemaField

pipeline = Processor("jdbc:postgresql:GitHubData", "program", "DatabaseAccess", load_percent=100)
schema = Unischema("GitHubProcessedData", [
    UnischemaField("name", "str", 1),
    UnischemaField("description", "str", 1),
    UnischemaField("topics", "str", 1),
    UnischemaField("primarylanguage", "str", 1),
    UnischemaField("license", "str", 1),
    UnischemaField("readme", "str", 1),
    UnischemaField("titles", "str", 1),
    UnischemaField("length", "int", 1),
    UnischemaField("createddate", "date", 1),
    UnischemaField("lastpusheddate", "date", 1),
    UnischemaField("fork", "bool", 1),
    UnischemaField("archived", "bool", 1),
    UnischemaField("locked", "bool", 1),
    UnischemaField("stars", "int", 1),
])

# Remove alternative popularity metrics (which aren't used)
drop = RemoveColumns("id", "forkcount", "watchs", "users", "issues", "pullrequests")

# Convert boolean values into integers (0's and 1's) to avoid problems when testing (caused by type casting)
cast_to_int = CastToInt("fork", "locked", "archived")

# Min-Max feature scaling
assembler = VectorAssembler(inputCols=["stars"], outputCol="starsVector")
scaler = MinMaxScaler(inputCol="starsVector", outputCol="target")

# Temporarily remove all text features (until they're used) and clean vector column
# drop.columns = [*drop.columns, "name", "description", "topics", "readme"]
extract_text_features = ExtractTextFeatures("readme")
cleanup = CleanUp("target")

# Only run preprocessing if the data doesn't already exist
train_path, test_path = Path("Data/Train.parquet"), Path("Data/Test.parquet")
if not train_path.exists() or not test_path.exists():
    pipeline.process_data(drop, assembler, scaler, cast_to_int, extract_text_features, cleanup, show=True)
    pipeline.save_processed_data(train_path, test_path, schema)

# Train models
automl = H2OSparkAutoML(pipeline.train_data, pipeline.test_data)
model, results = automl.train_models(max_runtime=120)
automl.visualise_model(results)
