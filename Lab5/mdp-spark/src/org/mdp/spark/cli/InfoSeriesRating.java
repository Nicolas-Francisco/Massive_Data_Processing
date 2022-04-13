package org.mdp.spark.cli;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

/*
 * Tuple for The Simpsons#1989:
 * The Simpsons#1989     Homer's Enemy (#8.23)     9.2     7.375168918918921
 *
 * Tuple of our choice: Friends#1994
 * Friends#1994     The Last One: Part 2 (#10.18)|The One Where Everybody Finds Out (#5.14)     9.7     8.507627118644063
 */


/**
 * Get the average ratings of TV series from IMDb.
 *
 * This is the Java 8 version with lambda expressions.
 */
public class InfoSeriesRating {

	/**
	 * This will be called by spark
	 */
	public static void main(String[] args) {

		if(args.length != 2) {
			System.err.println("Usage arguments: inputPath outputPath");
			System.exit(0);
		}
		new InfoSeriesRating().run(args[0],args[1]);
	}

	/**
	 * The task body
	 */
	public void run(String inputFilePath, String outputFilePath) {
		/*
		 * Initialises a Spark context with the name of the application
		 *   and the (default) master settings.
		 */
		SparkConf conf = new SparkConf()
				.setAppName(AverageSeriesRating.class.getName());
		JavaSparkContext context = new JavaSparkContext(conf);

		/*
		 * Load the first RDD from the input location (a local file, HDFS file, etc.)
		 */
		JavaRDD<String> inputRDD = context.textFile(inputFilePath);

		/*
		 * Here we filter lines that are not TV series or where no episode name is given
		 */
		JavaRDD<String> tvSeries = inputRDD.filter(
				line -> line.split("\t")[6].equals("TV_SERIES") && !line.split("\t")[7].equals("null")
		);

		/*
		 * We create a tuple (series,episode,rating) where series is the key (name+"#"+year+"#"+disambiguator)
		 */
		JavaRDD<Tuple3<String,String,Double>> seriesEpisodeRating = tvSeries.map(
				line -> new Tuple3<String,String,Double> (
						line.split("\t")[3] + "#" + line.split("\t")[4],
						line.split("\t")[7],
						Double.parseDouble(line.split("\t")[2])
				)
		);

		/*
		 * We save in cache this RDD to reuse it
		 */
		JavaRDD<Tuple3<String,String,Double>> seriesEpisodeRatingCache = seriesEpisodeRating.cache();

		/*
		 * We will produce a map: (series, (episode, rating))
		 */
		JavaPairRDD<String, Tuple2<String, Double>> seriesToEpisodesRating =  seriesEpisodeRating.mapToPair(
				tup -> new Tuple2<String, Tuple2<String, Double>>(
						tup._1(),
						new Tuple2<String, Double>(tup._2(), tup._3()))
		);

		/*
		 * We will find the series episodes with the maximum rating
		 */
		JavaPairRDD<String, Tuple2<String, Double>> maxRating = seriesToEpisodesRating.reduceByKey(
				(a, b) -> {
					if (a._2() > b._2()) {
						return new Tuple2<String, Double>(a._1(), a._2());
					} if (a._2() < b._2) {
						return new Tuple2<String, Double>(b._1(), b._2());
					} else {
						return new Tuple2<String, Double>(a._1() + "|" + b._1(), a._2());
					}
				}
		);

		/*
		 * Now we start to compute the average rating per series.
		 *
		 * We don't care about the episode name for now so to start with,
		 * from tuples (series,episode,rating)
		 * we will produce a map: (series,rating)
		 *
		 * (We could have done this directly from tvSeries,
		 *   except seriesEpisodeRating will be reused later)
		 */
		JavaPairRDD<String,Double> seriesToEpisodeRating = seriesEpisodeRatingCache.mapToPair(
				tup -> new Tuple2<String,Double> (
						tup._1(),
						tup._3()
				)
		);

		/*
		 * To compute the average rating for each series, the idea is to
		 * maintain the following tuples:
		 *
		 * (series,(sum,count))
		 *
		 * Where series is the series identifier,
		 *   count is the number of episode ratings thus far
		 *   sum is the sum of episode ratings thus far
		 *
		 * Base value: (0,0)
		 *
		 * To combine (sum, count) | rating:
		 *   (sum+rating,count+1)
		 *
		 * To reduce (sum1,count1) | (sum2,count2)
		 *   (sum1+sum2,count1+count2)
		 */
		JavaPairRDD<String, Tuple2<Double, Integer>> seriesToSumCountRating =
				seriesToEpisodeRating.aggregateByKey(
						new Tuple2<Double, Integer>(0d, 0), // base value
						(sumCount, rating) ->
								new Tuple2<Double, Integer>(sumCount._1 + rating, sumCount._2 + 1 ), // combine function
						(sumCountA, sumCountB) ->
								new Tuple2<Double, Integer>(sumCountA._1 + sumCountB._1, sumCountA._2 + sumCountB._2 )); // reduce function

		/*
		 * We calculate the average rating for each series
		 */
		JavaPairRDD<String,Double> seriesToAvgRating = seriesToSumCountRating.mapToPair(
				tup -> new Tuple2<String,Double>(tup._1,tup._2._1/tup._2._2)
		);

		/*
		 *  We join the previous RDD with the one with the average ratings
		 */
		JavaPairRDD<String, Tuple2<Tuple2<String, Double>, Double>> joinTuplesRating = maxRating.join(seriesToAvgRating);

		/*
		 * We sort the join
		 */
		JavaPairRDD<String, Tuple2<Tuple2<String, Double>, Double>> joinTuplesRatingSorted = joinTuplesRating.sortByKey(true);

		/*
		 * Required output
		 */
		JavaRDD<String> finalOutput = joinTuplesRatingSorted.map(
				tup -> tup._1() + "     " + tup._2._1._1 + "     " + tup._2._1._2 + "     " + tup._2._2
		);

		/*
		 * Write the output to local FS or HDFS
		 */
		finalOutput.saveAsTextFile(outputFilePath);

		context.close();
	}
}