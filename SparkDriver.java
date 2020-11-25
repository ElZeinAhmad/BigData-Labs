package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class SparkDriver {

	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		HashMap<String, List<String>> pairUidPid = new HashMap<String, List<String>>();
		List<Tuple2<String, List<String>>> listpair = new ArrayList<Tuple2<String, List<String>>>();
		// Create a configuration object and set the name of the application
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #6");

		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the
		// cluster
		SparkConf conf = new SparkConf().setAppName("Spark Lab #6").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath);

		JavaRDD<String> inputRDDWout1Line = inputRDD.filter(e -> {
			String[] split = e.split(",");
			if (StringUtils.isNumeric(split[0]))
				return true;
			else
				return false;
		});

		for (String e : inputRDDWout1Line.collect()) {
			String[] splittedline = e.split(",");
			if (pairUidPid.isEmpty()) {
				List<String> list = new ArrayList<String>();
				list.add(splittedline[1]);
				pairUidPid.put(splittedline[2], list);

			}

			else {

				List<String> list = pairUidPid.get(splittedline[2]);
				if (list == null) {
					list = new ArrayList<String>();
					list.add(splittedline[1]);
					pairUidPid.put(splittedline[2], list);
				} else {
					if (!list.contains(splittedline[1])) {
						list.add(splittedline[1]);
						pairUidPid.put(splittedline[2], list);
					}
				}
			}
		}


		pairUidPid.entrySet().forEach(e -> {
			listpair.add(new Tuple2<String, List<String>>(e.getKey(), e.getValue()));
		});

		HashMap<String, Integer> PidOc = new HashMap<String, Integer>();

		JavaPairRDD<String, List<String>> UidPids = sc.parallelizePairs(listpair).sortByKey();

		for (Tuple2<String, List<String>> e : UidPids.collect()) {
			List<String> PidOcSin = new ArrayList<String>();
			for (String p : e._2) {
				if (PidOcSin.isEmpty()) {
					PidOcSin.add(p);
				} else {
					if (!PidOcSin.contains(p)) {
						PidOcSin.add(p);
					}

				}
			}
			for(int i=0;i<PidOcSin.size();i++) {
				for(int j=i+1;j<PidOcSin.size();j++) {
					if (PidOc.isEmpty()) {
						PidOc.put(PidOcSin.get(i)+" "+PidOcSin.get(j),1);
					}
					else
					{
						if(PidOc.containsKey(PidOcSin.get(i)+" "+PidOcSin.get(j))) {
							PidOc.put(PidOcSin.get(i)+" "+PidOcSin.get(j),PidOc.get(PidOcSin.get(i)+" "+PidOcSin.get(j))+1);
						}
						else {
							PidOc.put(PidOcSin.get(i)+" "+PidOcSin.get(j),1);
						}
					}
				
			}
		}
		}

		List<Tuple2<Integer,String>> ret = new ArrayList<>();
		PidOc.entrySet().forEach(e -> {
			ret.add(new Tuple2<Integer,String>(e.getValue(), e.getKey()));
		});

		JavaPairRDD<Integer,String> POccurCount = sc.parallelizePairs(ret);
		JavaPairRDD<Integer,String> lastPOcc = POccurCount.filter(e -> {
			if (e._1 > 1) {
				return true;
			} else {
				return false;
			}
		}).sortByKey(false);

//
//		for(Map.Entry <String,Integer> m : PidOc.entrySet())
//		{
//			ret.add(new Tuple2<String,Integer>(m.getKey(),m.getValue()));
//		}
//		return ret.iterator();
		lastPOcc.saveAsTextFile(outputPath);
		// Store the result in the output folder
		// resultRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
