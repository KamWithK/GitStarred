from pyspark.ml.pipeline import Transformer
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import length

# Wrapper classes for pipelines

class RemoveColumns(Transformer):
    def __init__(self, *columns):
        self.columns = columns

    def _transform(self, dataframe):
        return dataframe.drop(*self.columns)

class CastToInt(Transformer):
    def __init__(self, *columns):
        self.columns = columns
        
    def _transform(self, dataframe):
        for column in self.columns:
            dataframe = dataframe.withColumn(column, dataframe[column].cast("int"))
            
        return dataframe

class ExtractTextFeatures(Transformer):
    def __init__(self, column):
        self.column = column

    def _transform(self, dataframe):
        # Old regex: ^[# ]+(.*)\b
        return dataframe.withColumn("titles", regexp_extract(self.column, "^#+.*", 0)).withColumn("length", length(self.column))

class CleanUp(Transformer):
    def __init__(self, column, extras=[]):
        self.column, self.extras = column, ["stars", "starsVector", *extras]
        
    def _transform(self, dataframe):
        # Rename to avoid bug
        dataframe.withColumnRenamed(self.column, self.column)
        
        # Convert vector to column of doubles and remove the extra data
        return dataframe.withColumn(self.column, vector_to_array(dataframe[self.column]).getItem(0)).drop(*self.extras)
