#!/bin/bash

MODE=$1

if [[ -z "$MODE" ]]; then
    echo -n "Select execution mode: 1=local, 2=demo-hadoop, 3=full-hadoop [default: 1]: "
    read MODE
    MODE=${MODE:-1}
fi

if [[ "$MODE" == "2" || "$MODE" == "demo-hadoop" ]]; then
    echo "Running on Hadoop (Demo)..."
    python main.py -c ./pipeline_mrjob.conf -r hadoop hdfs:///user/e11736088/Ex1/reviews_devset.json --stopwords hdfs:///user/e11736088/Ex1/stopwords.txt > output_demo.txt

elif [[ "$MODE" == "3" || "$MODE" == "full-hadoop" ]]; then
    echo "Running on Hadoop (Full)..."
    python main.py -c ./pipeline_mrjob.conf -r hadoop hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json --stopwords hdfs:///user/e11736088/Ex1/stopwords.txt > output.txt

else
    echo "Running locally..."
    python main.py -c ./pipeline_mrjob.conf Assets/reviews_devset.json --stopwords Assets/stopwords.txt > output_local.txt
fi