package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }

    public int run(String args[]) {
        try {
            double lr = 0.001;
            double m = 0;
            double b = 0;

            for (int i = 0; i < 101; i++) {
                System.out.println("M at iteration " + i + ": " + m);
                System.out.println("B at iteration " + i + ": " + b);

                Configuration conf = new Configuration();
                conf.set("current.m", Double.toString(m));
                conf.set("current.b", Double.toString(b));
                conf.setInt("current.iteration", i);

                Job job = Job.getInstance(conf, "WordCount");
                job.setJarByClass(WordCount.class);
                job.setMapperClass(WordCountMapper.class);
                job.setReducerClass(WordCountReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Wrapper.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                job.setInputFormatClass(TextInputFormat.class);
                FileOutputFormat.setOutputPath(job, new Path(args[1] + i));
                job.setOutputFormatClass(TextOutputFormat.class);

                if (!job.waitForCompletion(true)) {
                    return 1;
                }

                // Read results from the job's counters
                double mpart = job.getCounters().findCounter("GradientDescent", "mpart").getValue() / 1000000.0;
                double bpart = job.getCounters().findCounter("GradientDescent", "bpart").getValue() / 1000000.0;

                m -= mpart * lr;
                b -= bpart * lr;
            }

            System.out.println("Final M: " + m);
            System.out.println("Final B: " + b);

            return 0;
        } catch (InterruptedException | ClassNotFoundException | IOException e) {
            System.err.println("Error during mapreduce job.");
            e.printStackTrace();
            return 2;
        }
    }
}