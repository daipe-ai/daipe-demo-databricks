import sys
import mlflow
from mlflow.tracking import MlflowClient


def transition_model_to(stage, model):
    client = MlflowClient()
    client.transition_model_version_stage(name=model.name, version=model.version, stage=stage)

    
def main():
    print("Running promotion")
    
    if len(sys.argv) != 2:
        raise Exception(f"Usage {argv[0]} <model_name>")

    model_name = sys.argv[1]
    
    mlflow.set_tracking_uri("databricks")
    client = MlflowClient()
    
    production_model = client.get_latest_versions(model_name, ["Production"])[0]
    staging_model = client.get_latest_versions(model_name, ["Staging"])[0]

    transition_model_to("Production", staging_model)
    print(f"Staging model version {staging_model.version} successfully promoted")
    
    transition_model_to("Archived", production_model)
    print(f"Production model version {production_model.version} successfully archived")

    
if __name__ == "__main__":
    main()
