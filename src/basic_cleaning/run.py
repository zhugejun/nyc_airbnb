#!/usr/bin/env python
"""
Performs basic data cleaning and save results to W&B
"""
import argparse
import logging
import wandb

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info('Downloading artifact')
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)


    logger.info('Dropping outliers')
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    logger.info('Change last_review to DateTime')
    df['last_review'] = pd.to_datetime(df['last_review'])

    logger.info('Saving cleaned data to lcoal')
    df.to_csv('clean_sample.csv', index=False)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help='Fully qualified name for the input artifact',
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help='Name of the output artifact to create',
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help='Type of the output artifact',
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help='Description of the output artifact',
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help='Minimum price for the airbnb stay',
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help='Maximum price for the airbnb stay',
        required=True
    )


    args = parser.parse_args()

    go(args)
