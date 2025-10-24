package edu.univ.haifa.bigdata;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NotificationsMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text appName = new Text();
    private final IntWritable notifications = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length >= 4 && !fields[3].equalsIgnoreCase("Notifications")) {
            appName.set(fields[1]); // App name
            notifications.set(Integer.parseInt(fields[3])); // Notifications count
            context.write(appName, notifications);
        }
    }
}
