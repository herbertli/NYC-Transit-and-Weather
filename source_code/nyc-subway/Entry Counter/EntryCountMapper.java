import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.lang.*;

public class EntryCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> { 
    
    @Override
    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        //String scp = fields[2];
        String timeStamp = fields[7];
	String date = fields[6];
	String line_Val = fields[4];
	String location = fields[3];
	String scp_date = date+";"+location+";"+line_Val+";"+timeStamp;
        String entries2 = fields[10].trim();
	LongWritable entries = new LongWritable(Long.parseLong(entries2));
	//String exits = fields[10];
	//String entries_exits = entries+";"+exits;
        context.write(new Text(scp_date), entries);
    }
}
