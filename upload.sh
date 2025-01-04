#!/bin/bash
ssh lsd hdfs dfs -rm -r hdfs://lsd-prod/user/ychancrin/out.file
scp main.py lsd:/home/ychancrin/proj_twitter
ssh lsd /home/ychancrin/proj_twitter/submit.sh