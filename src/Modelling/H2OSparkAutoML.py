import sys, os
import h2o

from pysparkling import H2OContext
from pysparkling.ml import H2OAutoML

# Function to avoid uneccessary outputs being printed to terminal
def deafen(function, *args):
    real_stdout = sys.stdout
    sys.stdout = open(os.devnull, "w")
    output = function(*args)
    sys.stdout = real_stdout
    return output

class H2OSparkAutoML():
    def __init__(self, train_data, test_data):
        self.train_data, self.test_data = train_data, test_data

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
