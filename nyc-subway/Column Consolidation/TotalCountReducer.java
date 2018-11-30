import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.lang.*;
import java.io.*;
import java.util.*;


public class TotalCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
	long big = 0;
	long a = 0;
	long small = 0;
        for (LongWritable value: values) {
	    big = value.get();
	    a = big+small;
	    small = big;      
        }
    LongWritable p = new LongWritable(Math.abs(a));
    //String p = Integer.toString(a);
    context.write(key, p); //print out in (key,value) form
    }
}
