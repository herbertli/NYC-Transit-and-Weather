import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.lang.*;
import java.io.*;
import java.util.*;
import java.text.*;


public class StatsCountReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text val: values){
	
	    String line1 = val.toString();
            String[] fields1 = line1.split(";");// should be in timestamp,counter format.
	    java.sql.Timestamp ts1 = java.sql.Timestamp.valueOf(fields1[0].trim());
	    long ts_1 = ts1.getTime(); // First time ready.
	    long counter1 = Long.parseLong(fields1[1].trim());
	    
		for (Text val2:values){
		String line2 = val2.toString();
                String[] fields2 = line2.split(";");
		long counter2 = Long.parseLong(fields2[1].trim());
                java.sql.Timestamp ts2 = java.sql.Timestamp.valueOf(fields2[0].trim());
                long ts_2 = ts2.getTime(); // Second time ready.

		if((ts_2 - ts_1 < 21600000) && (ts_2 - ts_1 >= 0)){
		    long counter3 = counter2 - counter1; // row-wise difference.
		    String key_str = key.toString();
		    String key_intm = key_str+";"+fields2[0]+ " ("+key_str+";"+fields1[0]+") ";
		    Text key2 = new Text(key_intm);
		    Text val3 = new Text(Long.toString(counter3));
		
    		    context.write(key2, val3); //print out in (key,value) form
      		   }
    	    }
  	}	
    }
}
