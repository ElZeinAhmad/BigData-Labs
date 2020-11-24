package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);


		String inputPath;
		String outputPath;
		String prefix;
		
		inputPath=args[0];
		outputPath=args[1];
		prefix=args[2];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab #5").setMaster("local");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #5").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder
		// Each element/string of wordFreqRDD corresponds to one line of the input data 
		// (i.e, one pair "word\tfreq")  
		JavaRDD<String> wordFreqRDD = sc.textFile(inputPath);

		/*
		 * Task 1
		*/
		JavaRDD<String> matchedPref = wordFreqRDD.filter(e->e.startsWith(prefix));
		System.out.println(matchedPref.count());
		JavaPairRDD<String,Integer> matchedPair = matchedPref.mapToPair(e->{
			String[] str = e.toString().split("\\s+");
			return new Tuple2<String,Integer>(str[0],new Integer(str[1]));
		});
		Integer x = matchedPair.values().reduce((e1,e2)->{
			if(e1>e2)
			return e1;
			else
		    return e2;
		});
		System.out.println(x);
		/*
		 * Task 2
		*/
		
		JavaRDD<String> matched80per = matchedPref.filter(e->{
			String[] split = e.split("\\s+");
			if(new Integer(split[1])>(0.8*x))
				return true;
			else
				return false;
		});
		System.out.println(matched80per.count());
		JavaRDD<String> matched80perOnlyWord = matched80per.map(e->{
			String[] split = e.split("\\s+");
			return split[0];
		});
		matched80perOnlyWord.saveAsTextFile(outputPath);
		// Close the Spark context
		sc.close();
	}
}
