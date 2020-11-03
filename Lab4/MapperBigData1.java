package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    		
    		String[] line = value.toString().split(",");
    		if(StringUtils.isNumeric(line[0])) {
    			String ProductId = line[1];
        		String UserId = line[2];
        		int score = Integer.parseInt(line[6]);
        		context.write(new Text(UserId),new Text(ProductId + " " + score));
    		}
    		
    		
    }
}
