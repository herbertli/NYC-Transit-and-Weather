import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;

class LocationTimeMapper {

    private static final String TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TIMESTAMP_PATTERN);

    static class YellowMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String[] rowSplit = value.toString().split(",");
            if (rowSplit.length < 3) return;
            ArrayList<String> rowList = DataSchema.extractYellow(rowSplit);

            if (rowList == null) {
                return;
            }
            String cleanedRow = String.join(",", rowList);
            String[] split_line = cleanedRow.split(",");

            LocalDateTime pickupTime;
            LocalDateTime dropoffTime;

            // check for header rows...
            try {
                pickupTime = LocalDateTime.parse(split_line[0], formatter);
                dropoffTime = LocalDateTime.parse(split_line[1], formatter);
            } catch (DateTimeParseException e) {
                return;
            }
            String pickupDT = split_line[0];
            String dropoffDT = split_line[1];

//            int pDayOfMonth = pickupTime.getDayOfMonth();
//            int pYear = pickupTime.getYear();
//            int pMonth = pickupTime.getMonthValue();
//            int pHourOfDay = pickupTime.getHour();
//            int pMinuteOfHour = pickupTime.getMinute();
//            String pickupDT = String.format("%d,%d,%d,%d,%d", pYear, pMonth, pDayOfMonth, pHourOfDay, pMinuteOfHour);
//
//            int dDayOfMonth = dropoffTime.getDayOfMonth();
//            int dYear = dropoffTime.getYear();
//            int dMonth = dropoffTime.getMonthValue();
//            int dHourOfDay = dropoffTime.getHour();
//            int dMinuteOfHour = dropoffTime.getMinute();
//            String dropoffDT = String.format("%d,%d,%d,%d,%d", dYear, dMonth, dDayOfMonth, dHourOfDay, dMinuteOfHour);

            int numPassenger;
            if (StringUtils.isNumeric(split_line[2]))
                numPassenger = Integer.parseInt(split_line[2]);
            else
                return;

            String distance = split_line[3];

            String pickupLoc = split_line[4];
            if (!StringUtils.isNumeric(split_line[4]))
                pickupLoc = "-1";

            String dropoffLoc = split_line[5];
            if (!StringUtils.isNumeric(split_line[5]))
                dropoffLoc = "-1";

            String outKey = String.join(",", pickupDT, dropoffDT, distance, pickupLoc, dropoffLoc);
            context.write(new Text(outKey), new LongWritable(numPassenger));
        }
    }

    static class GreenMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] rowSplit = value.toString().split(",");
            if (rowSplit.length < 3) return;
            ArrayList<String> rowList = DataSchema.extractGreen(rowSplit);

            if (rowList == null) {
                return;
            }
            String cleanedRow = String.join(",", rowList);
            String[] split_line = cleanedRow.split(",");

            LocalDateTime pickupTime;
            LocalDateTime dropoffTime;

            // check for header rows...
            try {
                pickupTime = LocalDateTime.parse(split_line[0], formatter);
                dropoffTime = LocalDateTime.parse(split_line[1], formatter);
            } catch (DateTimeParseException e) {
                return;
            }
            String pickupDT = split_line[0];
            String dropoffDT = split_line[1];

//            int pDayOfMonth = pickupTime.getDayOfMonth();
//            int pYear = pickupTime.getYear();
//            int pMonth = pickupTime.getMonthValue();
//            int pHourOfDay = pickupTime.getHour();
//            int pMinuteOfHour = pickupTime.getMinute();
//            String pickupDT = String.format("%d,%d,%d,%d,%d", pYear, pMonth, pDayOfMonth, pHourOfDay, pMinuteOfHour);
//
//            int dDayOfMonth = dropoffTime.getDayOfMonth();
//            int dYear = dropoffTime.getYear();
//            int dMonth = dropoffTime.getMonthValue();
//            int dHourOfDay = dropoffTime.getHour();
//            int dMinuteOfHour = dropoffTime.getMinute();
//            String dropoffDT = String.format("%d,%d,%d,%d,%d", dYear, dMonth, dDayOfMonth, dHourOfDay, dMinuteOfHour);

            int numPassenger;
            if (StringUtils.isNumeric(split_line[4]))
                numPassenger = Integer.parseInt(split_line[4]);
            else
                return;

            String pickupLoc = split_line[2];
            if (!StringUtils.isNumeric(split_line[2]))
                pickupLoc = "-1";

            String dropoffLoc = split_line[3];
            if (!StringUtils.isNumeric(split_line[3]))
                dropoffLoc = "-1";

            String distance = split_line[5];

            String outKey = String.join(",", pickupDT, dropoffDT, distance, pickupLoc, dropoffLoc);
            context.write(new Text(outKey), new LongWritable(numPassenger));
        }
    }

    static class FHVMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final String TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss";
        private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TIMESTAMP_PATTERN);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] rowSplit = value.toString().replace("\"", "").split(",");
            if (rowSplit.length != 5) return;

            String pickupDT = rowSplit[1].replace("\"", "");
            try {
                LocalDateTime pickupTime = LocalDateTime.parse(rowSplit[1], formatter);
            } catch (Exception e) {
                return;
            }

            String dropoffDT = rowSplit[2].replace("\"", "");
            if (pickupDT.length() == 0) return;
            if (dropoffDT.length() == 0) dropoffDT = pickupDT;

//            int pDayOfMonth = pickupTime.getDayOfMonth();
//            int pYear = pickupTime.getYear();
//            int pMonth = pickupTime.getMonthValue();
//            int pHourOfDay = pickupTime.getHour();
//            int pMinuteOfHour = pickupTime.getMinute();
//            String pickupDT = String.format("%d,%d,%d,%d,%d", pYear, pMonth, pDayOfMonth, pHourOfDay, pMinuteOfHour);
//
//            int dDayOfMonth = dropoffTime.getDayOfMonth();
//            int dYear = dropoffTime.getYear();
//            int dMonth = dropoffTime.getMonthValue();
//            int dHourOfDay = dropoffTime.getHour();
//            int dMinuteOfHour = dropoffTime.getMinute();
//            String dropoffDT = String.format("%d,%d,%d,%d,%d", dYear, dMonth, dDayOfMonth, dHourOfDay, dMinuteOfHour);
//
            String pickupLoc = rowSplit[3];
            if (pickupLoc.length() == 0 || !StringUtils.isNumeric(rowSplit[3]))
                pickupLoc = "-1";

            String dropoffLoc = rowSplit[4];
            if (dropoffLoc.length() == 0 || !StringUtils.isNumeric(rowSplit[4]))
                dropoffLoc = "-1";

            String outKey = String.join(",", pickupDT, dropoffDT, pickupLoc, dropoffLoc);

            context.write(new Text(outKey), new LongWritable(1));

        }
    }

}
