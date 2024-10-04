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
            double m1 = 0, m2 = 0, m3 = 0, m4 = 0, b = 0;

            for (int i = 0; i < 100; i++) {
                Configuration conf = new Configuration();
                conf.set("current.m1", Double.toString(m1));
                conf.set("current.m2", Double.toString(m2));
                conf.set("current.m3", Double.toString(m3));
                conf.set("current.m4", Double.toString(m4));
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
                double m1part = ((double)job.getCounters().findCounter("GradientDescent", "m1part").getValue() / 1000000.0);
                double m2part = ((double)job.getCounters().findCounter("GradientDescent", "m2part").getValue() / 1000000.0);
                double m3part = ((double)job.getCounters().findCounter("GradientDescent", "m3part").getValue() / 1000000.0);
                double m4part = ((double)job.getCounters().findCounter("GradientDescent", "m4part").getValue() / 1000000.0);
                double bpart = ((double) job.getCounters().findCounter("GradientDescent", "bpart").getValue() / 1000000.0);
                m1 -= lr * m1part;
                m2 -= lr * m2part;
                m3 -= lr * m3part;
                m4 -= lr * m4part;
                b -= lr * bpart;
                
                System.out.println("Partials: " + m1part + " " + m2part + " " + m3part + " " + m4part + "  " + b);
                System.out.println("Iteration " + i + ": m1=" + m1 + ", m2=" + m2 + ", m3=" + m3 + ", m4=" + m4 + ", b=" + b);
                System.out.println("Cost: " + ((double) job.getCounters().findCounter("GradientDescent", "cost").getValue() / 1000000.0));
            }

            System.out.println("Final m1: " + m1);
            System.out.println("Final m2: " + m2);
            System.out.println("Final m3: " + m3);
            System.out.println("Final m4: " + m4);
            System.out.println("Final b: " + b);

            return 0;
        } catch (InterruptedException | ClassNotFoundException | IOException e) {
            System.err.println("Error during mapreduce job.");
            e.printStackTrace();
            return 2;
        }
    }
}