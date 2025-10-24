package edu.univ.haifa.bigdata;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CorrelationMapper extends Mapper<Object, Text, Text, Text> {
    private Text appName = new Text();
    private Text usageNotifications = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length >= 4) {
            appName.set(fields[1]); // App name
            usageNotifications.set(fields[2] + "_" + fields[3]); // Usage_Notifications
            context.write(appName, usageNotifications);
        }
    }
}