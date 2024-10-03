package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, Wrapper, Text, DoubleWritable> {

   public void reduce(Text text, Iterable<Wrapper> values, Context context)
           throws IOException, InterruptedException {
        
        // Get parameters from the Configuration
        double m = Double.parseDouble(context.getConfiguration().get("current.m", "0"));
        double b = Double.parseDouble(context.getConfiguration().get("current.b", "0"));
        int iteration = context.getConfiguration().getInt("current.iteration", 0);

        double[] data = new double[3];
        double count = 0;

        for (Wrapper val : values) {
            count++;
            double distance = val.getDistance().get();
            double fare_amount = val.getFareAmount().get();
            data[0] += (-1 * distance) * (fare_amount - ((m * distance) + b));
            data[1] += -1 * (fare_amount - (m * distance + b));
            data[2] += Math.pow((fare_amount - ((m * distance) + b)), 2);
        }

        System.out.println(data[0] + " " + data[1] + " " + data[2]);

        double mpart = (2 / count) * data[0]; 
        double bpart = (2 / count) * data[1];

        System.out.println("mpart at iteration " + iteration + ": " + mpart);
        System.out.println("bpart at iteration " + iteration + ": " + bpart);
        System.out.println("Cost at iteration " + iteration + ": " + data[2]);

        // Use Hadoop Counters to pass the results back
        context.getCounter("GradientDescent", "mpart").increment((long)(mpart * 1000000));
        context.getCounter("GradientDescent", "bpart").increment((long)(bpart * 1000000));

        // Write the results
        context.write(new Text("m"), new DoubleWritable(mpart));
        context.write(new Text("b"), new DoubleWritable(bpart));
   }
}