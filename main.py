from pyspark import SparkContext, SparkConf
from argparse import ArgumentParser
from timeit import default_timer as timer
import json
from itertools import combinations
import operator


def tweet_has_hashtags(tweet): # -> bool:
	return "entities" in tweet and "hashtags" in tweet["entities"] and len(tweet["entities"]["hashtags"]) >= 2


def get_hashtags(tweet): # -> list[str]:
    return ['#' + it["text"] for it in tweet["entities"]["hashtags"]]


def get_hashtags_couples(tweet): # -> list[tuple(str, str)]:
	return combinations(sorted(set(get_hashtags(tweet))), 2)  # Set to eliminate repetitions


def swap(x):
	return (x[1], x[0])


def get_hashtags_couples_count(tweets):
	tweet_dict = tweets.map(json.loads).filter(tweet_has_hashtags)
	hashtags = tweet_dict.flatMap(get_hashtags_couples)
	hashtags_count = hashtags.map(lambda it: (it, 1)).reduceByKey(operator.add).map(swap)
	return hashtags_count


def build_main_rdd(inputs):
	main_rdd = sc.textFile(inputs[0])

	for in_file in inputs[1:]:
		tweets_rdd = sc.textFile(in_file)
		main_rdd = main_rdd.union(tweets_rdd)

	return main_rdd


if "__main__" ==__name__ :

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

	rdd = build_main_rdd(inputs)
	counts = get_hashtags_couples_count(rdd)

	counts = counts.sortByKey(ascending=False) 
	counts.saveAsTextFile(output)

	end = timer()


	print(f"Total time {end - start:.3f}s")


