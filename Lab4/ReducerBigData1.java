package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    TreeMap<String,String> Tm;
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	float sum=0;
    	float average=0;
    	int count=0;
    	Tm = new TreeMap<String,String>();
    	for(Text val:values)
    	{
    		String[] splitText = val.toString().split("\\s+");
    		Tm.put(splitText[0],splitText[1]);
    		sum=sum+Float.parseFloat(splitText[1]);
    		count++;   		
    	}
    	average=sum/count;
    	for(Map.Entry<String,String> e : Tm.entrySet()) {
    	context.write(new Text(e.getKey()),new Text(key.toString()+" " +(Float.parseFloat(e.getValue())-average)));
    	
    }
}
    }
