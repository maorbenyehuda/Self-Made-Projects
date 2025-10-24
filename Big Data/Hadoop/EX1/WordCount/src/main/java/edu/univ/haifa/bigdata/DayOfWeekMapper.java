package edu.univ.haifa.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class DayOfWeekMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text dayOfWeek = new Text();
    private final IntWritable launches = new IntWritable();

    private static final SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
    private static final SimpleDateFormat dayOfWeekFormat = new SimpleDateFormat("EEEE", Locale.ENGLISH);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        // Check if the row has enough fields (assuming at least 3 fields)
        if (fields.length >= 3) {
            try {
                String dateStr = fields[0].trim();  // Assuming the date is in the first column
                String timesOpenedStr = fields[2].trim(); // Assuming "Times Opened" is the third column

                // Convert date to day of the week
                Date date = inputDateFormat.parse(dateStr);
                String day = dayOfWeekFormat.format(date);

                // Parse the "Times Opened" field as an integer
                int timesOpened = Integer.parseInt(timesOpenedStr);

                // Write day of the week and number of launches to context
                dayOfWeek.set(day);
                launches.set(timesOpened);
                context.write(dayOfWeek, launches);

            } catch (ParseException e) {
                System.err.println("Skipping row due to invalid date format: " + line);
            } catch (NumberFormatException e) {
                System.err.println("Skipping row due to invalid number format: " + line);
            }
        } else {
            System.err.println("Skipping row due to insufficient fields: " + line);
        }
    }
}
