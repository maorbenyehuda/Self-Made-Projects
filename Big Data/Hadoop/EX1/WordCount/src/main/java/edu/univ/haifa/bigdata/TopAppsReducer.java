package edu.univ.haifa.bigdata;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class TopAppsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final TreeMap<Integer, String> topApps = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalUsage = 0;
        for (IntWritable val : values) {
            totalUsage += val.get();
        }
        topApps.put(totalUsage, key.toString());

        if (topApps.size() > 5) {
            topApps.pollFirstEntry(); // Keep only the top 5 apps
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, String> entry : topApps.descendingMap().entrySet()) {
            context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
        }
    }
}
