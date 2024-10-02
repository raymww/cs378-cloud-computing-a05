package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Comparator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private double m;
	private double b;
	private int iteration;

    public void configure(JobConf job) {
        m = Double.parseDouble(job.get("m", "")); 
        // nb: last arg is the default value if option is not set
        b = Double.parseDouble(job.get("b", ""));
		iteration = job.getInt("iteration", iteration);
    }

    public void reduce(Text text, Iterable<Wrapper> values, Context context)
           throws IOException, InterruptedException {
	   
        double[] data = new double[3];
        int count = 0;
        
        for (Wrapper val : values) {
            count++;
            // System.out.println("Taxi ID: " + text.toString() + " Time: " + val.gettime().get() + " Total: " + val.gettotal().get());
            double distance = val.getDistance().get();
            double fare_amount = val.getFareAmount().get();
            // float multiplied = val.getMultiplied().get();
            data[0] += (-1 * distance) * (fare_amount - (m * distance  + b));
            data[1] += -1 * (fare_amount - (m * distance  + b));
            data[2] += Math.pow((fare_amount - (m * distance  + b)), 2);        
        }

        double mpart = (2 / count) * data[0];
        double bpart = (2 / count) * data[1];

        System.out.println("Cost at iteration " + iteration + ": " + data[2]);

        context.write(new Text("m"), new DoubleWritable(mpart));
        context.write(new Text("b"), new DoubleWritable(bpart));
    }
}