from pyspark import SparkContext, SparkConf
from argparse import ArgumentParser
from timeit import default_timer as timer
import json
from itertools import combinations
import operator

prog_name = "Tweets Analysis"
conf = SparkConf().setAppName(prog_name)
sc = SparkContext(conf=conf)

# sc.setLogLevel("ERROR")

parser = ArgumentParser(
	prog=prog_name,
	description='Tweets tags analyser'
)

parser.add_argument("files", nargs="+")

args = parser.parse_args()

if len(args.files) < 2:
	parser.error("Need at least 1 tweet input file and 1 output file")
	exit(1)

output = args.files[-1]
inputs = args.files[:-1]

start = timer()

main_rdd = None

tweets_file = inputs[0]


def tweet_has_hashtags(tweet): # -> bool:
	return "entities" in tweet and "hashtags" in tweet["entities"] and len(tweet["entities"]["hashtags"]) >= 2


def get_hashtags(tweet): # -> list[str]:
    return ['#' + it["text"] for it in tweet["entities"]["hashtags"]]


def get_hashtags_couples(tweet): # -> list[tuple(str, str)]:
	return combinations(sorted(get_hashtags(tweet)), 2)


def swap(x):
	return (x[1], x[0])


def get_hashtags_couples_count(in_file):
	tweets = sc.textFile(in_file)
	tweet_dict = tweets.map(json.loads).filter(tweet_has_hashtags)
	hashtags = tweet_dict.flatMap(get_hashtags_couples)
	hashtags_count = hashtags.map(lambda it: (it, 1)).reduceByKey(operator.add).map(swap)
	return hashtags_count


for in_file in inputs:
	counts = get_hashtags_couples_count(in_file)
	print(counts.top(10))
	print(counts.count())

end = timer()


print(f"Total time {end - start:.3f}s")


