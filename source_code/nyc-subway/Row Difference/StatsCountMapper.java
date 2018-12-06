import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.lang.*;
import java.io.*;
import java.util.*;


public class StatsCountMapper extends Mapper<LongWritable, Text, Text, Text> { 
    
    @Override
    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
        String line = value.toString();
        List<String> fields = Arrays.asList(line.split("\\s"));
	List<String> fields2 = Arrays.asList(line.split(";"));
	//String b = "";
	//for(int i =0;i<fields.size() - 1;i++){
	//    b = b + fields.get(i);
	//}
	//String[] fields2 = b.split(";");
	String keyval = fields2.get(1)+";"+fields2.get(2); //key with station and line
	String entries2 = fields.get(fields.size() - 1);// the counter value
	String date = fields2.get(0); //old date
	String[] dateFields = date.split("/");
	String newDate = dateFields[2]+"-"+dateFields[0]+"-"+dateFields[1];//new formulated date
	String[] onlyTime = (fields2.get(3)).split("\\s");
	String datetime = newDate+" "+onlyTime[0]+";"+entries2;// date,time and count
	
	//LongWritable entries = new LongWritable(Long.parseLong(entries2));
	//String exits = fields[10];
	//String entries_exits = entries+";"+exits;
        context.write(new Text(keyval), new Text(datetime));
    }
}
