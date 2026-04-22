# Assignment 1 – Text Processing Fundamentals using MapReduce

## Project Overview

This project processes Amazon review data using Python MRJob and Hadoop-compatible execution. The objective is to compute Chi-Square (χ²) scores for unigram terms and select the top 75 most discriminative terms for each category.

## Project Structure

* `main.py`
* `run_pipeline.sh`
* `pipeline_mrjob.conf`
* `requirements.txt`
* `output.txt`
* `Assets/`

## Requirements

* Python 3
* mrjob
* Hadoop cluster / local mode

Install dependencies:

```bash
pip install -r requirements.txt
```

## Input Files

* `reviews_devset.json`
* `reviewscombined.json`
* `stopwords.txt`

## Preprocessing

* Tokenization to unigrams
* Lowercase conversion
* Stopword removal
* Remove one-character tokens

## How to Run

### Local Mode

```bash
bash run_pipeline.sh
```

### Hadoop Devset Mode

```bash
bash run_pipeline.sh demo-hadoop
```

### Hadoop Full Dataset Mode

```bash
bash run_pipeline.sh full-hadoop
```

## Output

Produces `output.txt` containing:

* Top 75 ranked terms per category
* Chi-Square scores
* Merged dictionary

## Pipeline

Input Data → Preprocessing → Mapper → Reducer → Output

## Authors

* Doan Chau Tran (11736088)
* Kashir Hasnain (12350030)
* Sajjad Ahmad (12510832)
* Faisal Zada (12454530)
* Adil Shinwari (12451264)
