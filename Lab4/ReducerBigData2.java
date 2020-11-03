package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<Text, // Input key type
		Text, // Input value type
		Text, // Output key type
		FloatWritable> { // Output value type
	@Override
	protected void reduce(Text key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {

		float sum = 0;
		float average = 0;
		int count = 0;
		for (Text val : values) {
			String[] splitText = val.toString().split("\\s+");
			sum = sum + Float.parseFloat(splitText[1]);

			count++;
		}
		average = sum / count;

		context.write(new Text(key.toString()), new FloatWritable(average));

	}
}
