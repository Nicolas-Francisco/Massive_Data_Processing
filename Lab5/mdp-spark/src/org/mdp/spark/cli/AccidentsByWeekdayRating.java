package org.mdp.spark.cli;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import scala.Serializable;
import scala.Tuple1;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

/**
 * Get the average ratings of TV series from IMDb.
 * 
 * This is the Java 8 version with lambda expressions.
 */
public class AccidentsByWeekdayRating implements Serializable {
	/**
	 * This will be called by spark
	 */
	public static void main(String[] args) {
		
		if(args.length != 2) {
			System.err.println("Usage arguments: inputPath outputPath");
			System.exit(0);
		}
		new AccidentsByWeekdayRating().run(args[0],args[1]);
	}

	/**
	 * Function that gets the day as a number
	 * 1 is Sunday, and 7 is Saturday
	 */
	public static int getDayNumberFromDate(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		return cal.get(Calendar.DAY_OF_WEEK);
	}


	/**
	 * Function that parses a string into a Date type
	 */
	public Date parseDateFromString(String dateInString){
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date date = format.parse(dateInString);
			return date;
		} catch (ParseException e) {
			e.printStackTrace();
			return new Date();
		}
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
				.setAppName(AccidentsByWeekdayRating.class.getName());
		JavaSparkContext context = new JavaSparkContext(conf);

		/*
		 * Load the first RDD from the input location (a local file, HDFS file, etc.)
		 */
		JavaRDD<String> inputRDD = context.textFile(inputFilePath);

		/*
		 * Here we filter lines where no start_time is given
		 */
//		JavaRDD<String> USAccidents = inputRDD.filter(
//				line -> !line.split("\t")[2].equals("null")
//		);


		/*
		 * We create a tuple (ID, Severity, Start_Time, State, WeekDay), where ID is the key
		 */
		JavaRDD<Tuple5<String,String,String,String,Integer>> USAccidentsRating = inputRDD.map(
				line -> new Tuple5<String,String,String,String,Integer> (
							line.split(",")[0],
							line.split(",")[1],
							line.split(",")[2],
							line.split(",")[3],
							getDayNumberFromDate(parseDateFromString(line.split(",")[2]))
						)
		);

//		JavaRDD<Tuple4<String,String,String,String>> USAccidentsRating = USAccidents.map(
//				line -> new Tuple4<String,String,String,String> (
//						line.split("\t")[0],
//						line.split("\t")[1],
//						line.split("\t")[2],
//						line.split("\t")[3]
//				)
//		);



//		JavaPairRDD<Integer,Integer> USAccidentsWeekdays = USAccidentsRating.mapToPair(
//				tup -> new Tuple2<Integer,Integer> (
//						tup._5(),
//						1
//				)
//		);

//		JavaPairRDD<String,Integer> USAccidentsWeekdays = USAccidentsRating.mapToPair(
//				tup -> new Tuple2<String,Integer> (
//						tup._2(),
//						1
//				)
//		);

		/*
		 * To compute the sum of accidents, we need to reduce maintain the following tuples:
		 *
		 * (Weekday <int>, count <int>)
		 *
		 * Base value: (0,0)
		 */
//		JavaPairRDD<Integer, Tuple1<Integer>> USAccidentsWeekdayToSumCount =
//				USAccidentsWeekdays.aggregateByKey(
//						new Tuple1<Integer>(0),
//						(sumCount, weekday) ->
//								new Tuple1<Integer>(sumCount._1 + 1),
//						(sumPartialCounts1, sumPartialCounts2) ->
//								new Tuple1<Integer>(sumPartialCounts1._1 + sumPartialCounts2._1));


//		JavaPairRDD<String, Tuple1<Integer>> USAccidentsWeekdayToSumCount =
//				USAccidentsWeekdays.aggregateByKey(
//						new Tuple1<Integer, Integer>(0),
//						(sumCount, weekday) ->
//								new Tuple1<Integer>(sumCount._1 + 1),
//						(sumPartialCounts1, sumPartialCounts2) ->
//								new Tuple1<Integer>(sumPartialCounts1._1 + sumPartialCounts2._1));

//		JavaPairRDD<Integer,Integer> USAccidentsWeekdayRating = USAccidentsWeekdayToSumCount.mapToPair(
//				tup -> new Tuple2<Integer, Integer>(tup._1 , tup._2._1)
//		);

//		JavaPairRDD<String,Integer> USAccidentsWeekdayRating = USAccidentsWeekdayToSumCount.mapToPair(
//				tup -> new Tuple2<String, Integer>(tup._1 , tup._2._1)
//		);

//		USAccidentsWeekdayRating.saveAsTextFile(outputFilePath);

		USAccidentsRating.saveAsTextFile(outputFilePath);

		context.close();
	}
}
