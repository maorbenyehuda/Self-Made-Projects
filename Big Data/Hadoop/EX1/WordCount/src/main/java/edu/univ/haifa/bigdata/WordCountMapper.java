package edu.univ.haifa.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\\s+");

        int wordLenLimit = context.getConfiguration().getInt("wordLenLimit", 0);

        for (String w : values) {
            // emit (w, 1)
            if (w.length() >= wordLenLimit) {
                context.write(new Text(w.toLowerCase()), new IntWritable(1));
            }
        }
    }
}
