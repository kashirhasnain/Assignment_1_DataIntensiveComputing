#!/bin/bash

# Default mode
MODE=$1

# Prompt the user if no mode is provided
if [[ -z "$MODE" ]]; then
    read -p "Select execution mode: 1. local, 2. demo-hadoop, 3. full-hadoop (1/2/3) [default: 1]: " MODE
    MODE=${MODE:-1}
fi

if [[ "$MODE" == "2" || "$MODE" == "demo-hadoop" ]]; then
    echo "Running on Hadoop (Demo)..."
    python main.py -c pipeline_mrjob.conf -r hadoop hdfs:///user/e11736088/Ex1/review_devset.json --stopwords hdfs:///user/e11736088/Ex1/stopwords.txt > output_pipeline_demo.txt
elif [[ "$MODE" == "3" || "$MODE" == "full-hadoop" ]]; then
    echo "Running on Hadoop (Full)..."
    python main.py -c pipeline_mrjob.conf -r hadoop hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json --stopwords hdfs:///user/e11736088/Ex1/stopwords.txt > output_pipeline.txt
else
    echo "Running locally..."
    python main.py -c pipeline_mrjob.conf Assets/reviews_devset.json --stopwords Assets/stopwords.txt > output.txt
fi

MODE=$2