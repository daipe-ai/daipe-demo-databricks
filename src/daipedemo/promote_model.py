import mlflow
from mlflow.tracking import MlflowClient


def transition_model_to(stage, model):
    client = MlflowClient()
    client.transition_model_version_stage(name=model.name, version=model.version, stage=stage)


if __name__ == "__main__":
    print("Running promotion")
    mlflow.set_tracking_uri("databricks")

    model_name = "rfc_loan_default_prediction"
    client = MlflowClient()
    production_model = client.get_latest_versions(model_name, ["Production"])[0]
    staging_model = client.get_latest_versions(model_name, ["Staging"])[0]

    transition_model_to("Production", staging_model)
    print(f"Staging model version {staging_model.version} successfully promoted")
    transition_model_to("Archived", production_model)
    print(f"Production model version {production_model.version} successfully archived")
