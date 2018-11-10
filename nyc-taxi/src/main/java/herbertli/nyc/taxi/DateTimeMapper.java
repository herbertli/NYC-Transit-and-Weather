package herbertli.nyc.taxi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.time.*;

import java.io.IOException;
import java.time.format.DateTimeFormatter;

public class DateTimeMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final String TIMESTAMP_PATTERN = "dd/MMM/yyyy:HH:mm:ss";
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TIMESTAMP_PATTERN);
        String[] split_line = value.toString().split(",");
        LocalDate localDate = LocalDate.parse(split_line[0], formatter);
        context.write(new Text(localDate.toString()), one);
    }
}
