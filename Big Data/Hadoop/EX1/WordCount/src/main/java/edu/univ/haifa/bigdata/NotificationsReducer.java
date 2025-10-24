package edu.univ.haifa.bigdata;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NotificationsReducer extends Reducer<Text, IntWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalNotifications = 0;
        int count = 0;

        for (IntWritable val : values) {
            totalNotifications += val.get();
            count++;
        }

        double average = (double) totalNotifications / count;
        context.write(key, new Text(String.format("%.2f notifications/day", average)));
    }
}
