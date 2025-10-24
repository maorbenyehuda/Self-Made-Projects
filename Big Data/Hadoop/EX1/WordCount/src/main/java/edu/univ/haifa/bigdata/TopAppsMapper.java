package edu.univ.haifa.bigdata;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopAppsMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text appName = new Text();
    private final IntWritable usageMinutes = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length >= 3 && !fields[2].equalsIgnoreCase("Usage (minutes)")) {
            appName.set(fields[1]); // App name
            usageMinutes.set(Integer.parseInt(fields[2])); // Usage in minutes
            context.write(appName, usageMinutes);
        }
    }
}
