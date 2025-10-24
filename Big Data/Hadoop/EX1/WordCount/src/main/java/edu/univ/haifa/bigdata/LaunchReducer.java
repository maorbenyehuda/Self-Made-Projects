package edu.univ.haifa.bigdata;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LaunchReducer extends Reducer<Text, IntWritable, Text, Text> {
    private final Map<String, String> dateToAppMax = new HashMap<>();
    private final Map<String, Integer> dateToMaxLaunches = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String[] keyParts = key.toString().split("_");
        String date = keyParts[0];
        String app = keyParts[1];

        int totalLaunches = 0;
        for (IntWritable val : values) {
            totalLaunches += val.get();
        }

        dateToAppMax.putIfAbsent(date, app);
        dateToMaxLaunches.putIfAbsent(date, 0);

        if (totalLaunches > dateToMaxLaunches.get(date)) {
            dateToAppMax.put(date, app);
            dateToMaxLaunches.put(date, totalLaunches);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String date : dateToAppMax.keySet()) {
            context.write(new Text(date), new Text(dateToAppMax.get(date) + " (" + dateToMaxLaunches.get(date) + " times)"));
        }
    }
}
