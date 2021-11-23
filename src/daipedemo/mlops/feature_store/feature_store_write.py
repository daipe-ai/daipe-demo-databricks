# Databricks notebook source
@dl.notebook_function()
def write_features(logger: Logger, features_writer: DatabricksFeatureStoreWriter):
    """Write all the features to Feature Store at once"""
    
    a = [result.fillna(0) for result in features_storage.results]
    
    for i, _ in enumerate(features_storage.results):
        features_storage.results[i] = a[i]
  
    logger.info("Writing features...")
    features_writer.write_latest(features_storage)  # noqa: F821
    features_writer.write_historized(features_storage)  # noqa: F821
    
    logger.info("Features successfully written.")
