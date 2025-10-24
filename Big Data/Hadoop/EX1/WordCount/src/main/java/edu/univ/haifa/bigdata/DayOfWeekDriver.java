package edu.univ.haifa.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DayOfWeekDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: DayOfWeekDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Most Frequent App Launch Days");
        job.setJarByClass(DayOfWeekDriver.class);

        // הגדרת Mapper ו-Reducer
        job.setMapperClass(DayOfWeekMapper.class);
        job.setReducerClass(DayOfWeekReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // הגדרת קלט ופלט
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // ביצוע העבודה
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
