import requests
from databricks import feature_store

from collections import namedtuple
from daipecore.function.input_decorator_function import input_decorator_function
from featurestorebundle.feature.FeatureStore import FeatureStore
from injecta.container.ContainerInterface import ContainerInterface
from pyspark.sql import DataFrame

Args = namedtuple("Args", "eval_metric model_name entity_name id_column")


@input_decorator_function
def evaluate_models(eval_metric, model_name: str, entity_name: str, id_column: str) -> bool:
    def wrapper(container: ContainerInterface):
        args = Args(eval_metric, model_name, entity_name, id_column)

        feature_store: FeatureStore = container.get(FeatureStore)

        ids_and_labels = feature_store.get_latest(args.entity_name).select(args.id_column, "label")

        stage_metric = __get_model_metric(args, "Staging", ids_and_labels)
        prod_metric = __get_model_metric(args, "Production", ids_and_labels)

        return stage_metric >= prod_metric

    return wrapper


def __get_model_metric(args: Args, model_stage: str, df: DataFrame) -> float:
    """Get accuracy for specified model for given dataset."""

    model_uri = f"models:/{args.model_name}/{model_stage}"

    fs = feature_store.FeatureStoreClient()
    predictions = fs.score_batch(model_uri, df).select(args.id_column, "prediction")
    preds = predictions.select("prediction").toPandas()
    labels = df.select("label").toPandas()

    return args.eval_metric(labels, preds)


def promote_new_model():
    """Makes a github API call which runs a model promotion pipeline"""
    logger.info(f"Running production promotion pipeline for new model")

    headers = {f"Authorization": f"token {dbutils.secrets.get(scope='git', key='token')}"}
    git_repo_handle = "daipe-ai/daipe-demo-databricks"
    url = f"https://api.github.com/repos/{git_repo_handle}/actions/workflows/promote_model.yml/dispatches"

    requests.post(url, headers=headers, json={"ref": "master"})
