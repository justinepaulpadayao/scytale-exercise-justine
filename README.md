# Project Overview

This project showcases a quick solution designed for efficient data engineering, leveraging a structured approach to handle data extraction, transformation, and storage. Inspired by best practices in software development, the project structure and workflow are tailored to meet the demands of complex data processing tasks.

## Project Structure

The foundation of this project is built upon a well-organized directory structure, derived from a widely-adopted cookie-cutter template. This template is specifically chosen for its applicability in general data engineering projects, ensuring a standardized and intuitive layout.

## Requirements
- Install requirements.txt
- Spark is installed/configured in a Windows Environment so might need to replicate the same using this [link](https://sparkbyexamples.com/pyspark-tutorial/)

- Create a .env file in the root directory that contains the following:
  - GITHUB_API_TOKEN (value: API token of your account/application)
  - GITHUB_ORGANIZATION (value: Scytale-exercise)


## Key Components:

- **Source Code** (src/main/python/jobs): This directory is the heart of the project, housing the main functional scripts that drive the data processing pipeline.
  - **data_extraction.py**: A dedicated script for extracting data from GitHub repositories. It meticulously fetches data and stores it in a structured JSON format, ready for further processing.
  - **data_transformation.py**: This script takes the helm in aggregating and joining data. Utilizing PySpark's powerful capabilities, it transforms the raw JSON files into insightful, aggregated datasets.
- **Data Storage** (data/input and data/output): Our data management strategy is straightforward yet effective, with separate directories for input and output data.
  - **input**: Serves as the repository for raw files extracted directly from the GitHub API. It's the starting point of our data journey.
  - **output**: The destination for our processed data, featuring aggregated and refined datasets, ready for analysis or further processing.

## Quality Assurance

To maintain the highest standards of code quality and readability, the project incorporates pre-commit hooks. These hooks serve as an automated checkpoint, ensuring that all contributions adhere to predefined coding standards and best practices before integration.
