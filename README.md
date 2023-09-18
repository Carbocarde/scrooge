# Scrooge - The penny auction project

This repo contains code used for my [blog post](https://app.nulliq.dev/penny).

I'm releasing all the raw data and processing scripts, but I will be omitting the observer.
While I'm particularly proud of the observer code, I don't want to distribute a tool that will slam the API with random users' requests.

## Data

All data for this project is distributed via IPFS. If you found it useful, consider hosting these files as well to add redundancy and improve download times.

Raw data is used by the processor. Once downloaded, move the .parquet files to `/processor/data/raw`

[Raw data IPFS download link](https://ipfs.io/ipfs/QmU1yoZnvEfcM3SrVQUip86542LmZzQYUY7TMijSVzdAC4)

## Run commands

Data preprocessing: `cargo run --bin processor`

This generates the split files that are used by the training code.

See model.ipynb for the model training code.

## Code stuff

Create new migration: `sea-orm-cli migrate generate <migration_name>`
Regenerate entities: `sea-orm-cli generate entity -u postgres://root:root@localhost/penny -o entity/src`
