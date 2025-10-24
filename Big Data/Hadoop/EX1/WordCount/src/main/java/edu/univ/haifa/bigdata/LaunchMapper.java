package edu.univ.haifa.bigdata;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LaunchMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text dateAppKey = new Text();
    private final IntWritable launches = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length >= 5 && !fields[4].equalsIgnoreCase("Times Opened")) {
            String date = fields[0]; // Date
            String app = fields[1]; // App name
            dateAppKey.set(date + "_" + app);
            launches.set(Integer.parseInt(fields[4])); // Times Opened
            context.write(dateAppKey, launches);
        }
    }
}
