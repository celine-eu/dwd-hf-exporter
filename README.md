# dwd-hf-exporter

Download historical datasets from HF `openclimatefix/dwd-icon-eu`

Download all files in sequence, apply the `converter.export` function and save the results to S3.

If a file exists already in S3, the download is skipped.

## Setup

Copy `.env.example` to `.env` and configure the variables.

Run `uv sync` to install dependencies

Run `.venv/bin/activate` to load the local virtual env

## Running

Use `python ./dwd_hf_exporter/main.py` to download

## Configuration

See `./dwd_hf_exporter/main.py` for configuration params