import mlflow

from databricks import feature_store
from databricks.feature_store.training_set import TrainingSet

from datasciencefunctions.supervised import log_model_summary


__flavors = {
  "spark": mlflow.spark,
}


def stage_model(model_name: str, model_summary, training_set: TrainingSet):
  dbx_feature_store = feature_store.FeatureStoreClient()
  mlflow_client = mlflow.tracking.MlflowClient()
  
  with mlflow.start_run() as run:
      dbx_feature_store.log_model(
          model_summary["models"]["pipeline"],
          model_name,
          flavor=__flavors[model_summary["params"]["model_framework"]],
          training_set=training_set,
      )
      
      log_model_summary(model_summary)

      run_id = run.info.run_id
      print(f"Run ID: {run_id}")
      
      model_uri = f"runs:/{run_id}/{model_name}"
      model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

      mlflow_client.transition_model_version_stage(name=model_name, version=model_details.version, stage="Staging")
