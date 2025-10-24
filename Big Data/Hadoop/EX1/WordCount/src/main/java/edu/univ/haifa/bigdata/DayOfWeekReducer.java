package edu.univ.haifa.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DayOfWeekReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalLaunches = 0;

        // Debug: Print incoming key
        System.out.println("Processing key: " + key.toString());

        for (IntWritable value : values) {
            totalLaunches += value.get();
        }

        // Debug: Print final count
        System.out.println("Key: " + key.toString() + ", Total Launches: " + totalLaunches);

        context.write(key, new IntWritable(totalLaunches));
    }
}
