from __future__ import print_function

import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: InfoSeriesRating.py <filein> <fileout>", file=sys.stderr)
        sys.exit(-1)

    filein = sys.argv[1]
    fileout = sys.argv[2]

    # in pyspark shell start with:
    #   filein = "hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings-two.tsv"
    #   fileout = "hdfs://cm:9000/uhadoop2025/holaquetal/info-series-rating.py/"
    # and continue line-by-line from here

    spark = SparkSession.builder.appName("InfoSeriesRating").getOrCreate()

    input = spark.read.text(filein).rdd.map(lambda r: r[0])
    # to dump an RDD like input to the pyspark shell, use
    #   input.collect()
    # make sure not to do this for a large RDD
    
    lines = input.map(lambda line: line.split("\t"))

    tvSeries = lines.filter(lambda line: (line[4] == "tvSeries") and (line[5] != 'null'))

    seriesEpisodeRating = tvSeries.map(lambda line: (line[2] + "#" + line[3], (line[5], float(line[1]))))

    # average rating per series
    seriesToRating = seriesEpisodeRating.map(lambda tup: (tup[0], tup[1][1]))

    seriesToSumCountRating = seriesToRating.aggregateByKey((0.0, 0),
        lambda acc, rating: (acc[0] + rating, acc[1] + 1),
        lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))

    seriesToAvgRating = seriesToSumCountRating.mapValues(lambda acc: acc[0] / acc[1])

    # group episodes per series
    seriesToEpisodeList = seriesEpisodeRating.groupByKey().mapValues(list)

    # function to extract best episodes
    def bestEpisodesInfo(episodes):
        maxRating = max([rating for (_, rating) in episodes])
        bestEpisodes = [title for (title, rating) in episodes if rating == maxRating]
        bestEpisodesStr = "|".join(bestEpisodes)
        return (bestEpisodesStr, maxRating)

    seriesToBestEpisodes = seriesToEpisodeList.mapValues(bestEpisodesInfo)

    # join average ratings and best episode info
    finalOutput = seriesToBestEpisodes.join(seriesToAvgRating).map(
        lambda kv: (kv[0], kv[1][0][0], kv[1][0][1], kv[1][1]))

    # format
    formattedOutput = finalOutput.map(
        lambda x: f"{x[0]}\t{x[1]}\t{x[2]}\t{x[3]}")

    formattedOutput.saveAsTextFile(fileout)

    spark.stop()