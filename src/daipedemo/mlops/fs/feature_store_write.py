# Databricks notebook source
@dl.notebook_function()
def write_features(logger: Logger, features_writer: FeatureStoreWriter):
    """Write all the features to Feature Store at once"""
  
    logger.info("Writing features...")
    features_writer.write_latest(features_storage)  # noqa: F821
    features_writer.write_historized(features_storage)  # noqa: F821
    
    logger.info("Features successfully written.")
