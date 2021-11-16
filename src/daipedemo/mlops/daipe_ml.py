import mlflow
import os

from databricks import feature_store
from databricks.feature_store import FeatureLookup
from databricks.feature_store.training_set import TrainingSet

from datasciencefunctions.supervised import supervised_wrapper, log_model_summary


__flavors = {
  "spark": mlflow.spark,
}


def stage_model(model_name: str, model_summary, training_set):
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
      

def train_model(df, entity_name, id_column, selected_features, model_type):
    key = [id_column]
    dbx_feature_store = feature_store.FeatureStoreClient()

    feature_lookup = [
        FeatureLookup(
            table_name=f"{os.environ['APP_ENV']}_feature_store.features_{entity_name}_latest",
            feature_names=[feature for feature in selected_features],
            lookup_key=key,
        )
    ]

    training_set = dbx_feature_store.create_training_set(
        df=df.select("LoanId", "label"), feature_lookups=feature_lookup, label="label", exclude_columns=key
    )

    # train test split, hyperparameter space, metrics to log and evaluate model, mlflow is done automatically
    _, _, model_summary = supervised_wrapper(
        df=training_set.load_df().fillna(0),
        model_type=model_type,
        use_mlflow=False,
        label_col="label",
        params_fit_model={"max_evals": 1},
    )


    def get_table_row(metric_name, value) -> str:
      return f"""
      <tr>
          <td>{metric_name}</td>
          <td>{value:.3f}</td>
      </tr>
      """

    html = f"""
      <!doctype html>
            <html lang="en">
            <head>
              <meta charset="utf-8">
              <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.0/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-KyZXEAg3QhqLMpG8r+8fhAXLRk2vvoC2f3B09zVXn8CA5QIVfZOJ3BCsw2P0p/We" crossorigin="anonymous">
            </head>
            <body>
            <div class="grid">
              <div class="row">
                <div class="col-6">
              <table class="table table-sm small">
                    <thead>
                      <tr>
                        <th scope="col">Metric</th>
                        <th scope="col">Value</th>
                      </tr>
                    </thead>
                    <tbody>
                      {"".join(get_table_row(name, value) for name, value in model_summary['metrics'].items())}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
            </body>
          </html>
      """


    def get_display_html():
        import IPython

        ipython = IPython.get_ipython()

        if not hasattr(ipython, "user_ns") or "displayHTML" not in ipython.user_ns:
            raise Exception("displayHTML cannot be resolved")

        return ipython.user_ns["displayHTML"]

    displayHTML = get_display_html()
    displayHTML(html)
    displayHTML("<img src='http://datasciencestunt.com/wp-content/uploads/2021/05/unnamed.png'>")

    return model_summary, training_set
  