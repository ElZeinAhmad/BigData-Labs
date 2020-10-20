package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    //String start_with;
    String prefix;
	protected void setup(Context context)
    {
    	//start_with = new String(context.getConfiguration().get("start_with"));
    	prefix = new String(context.getConfiguration().get("prefix"));

    }
	
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    	if(key.toString().contains(prefix))
    	{
    		context.write(key,value);
    	}
    }
}
