import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.lang.*;
import java.io.*;
import java.util.*;


public class TotalCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> { 
    
    @Override
    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
        String line = value.toString();
        List<String> fields = Arrays.asList(line.split("\\s"));
	String b = "";
	for(int i =0;i<fields.size() - 1;i++){
	    b = b + fields.get(i);
	}
	String entries2 = fields.get(fields.size() - 1);
	LongWritable entries = new LongWritable(Long.parseLong(entries2));
	//String exits = fields[10];
	//String entries_exits = entries+";"+exits;
        context.write(new Text(b), entries);
    }
}
