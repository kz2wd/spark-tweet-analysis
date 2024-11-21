#!/bin/bash
hdfs dfs -rm -r -f out.file 
spark-submit --master yarn $(dirname "$(realpath ${BASH_SOURCE[0]})")/main.py /user/fzanonboito/CISD/tiny_twitter.json out.file

