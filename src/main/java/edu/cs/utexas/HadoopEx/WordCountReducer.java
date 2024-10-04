package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, Wrapper, Text, DoubleWritable> {

   public void reduce(Text text, Iterable<Wrapper> values, Context context)
           throws IOException, InterruptedException {
        
        // Get parameters from the Configuration
        double m1 = Double.parseDouble(context.getConfiguration().get("current.m1", "0")); //amount
        double m2 = Double.parseDouble(context.getConfiguration().get("current.m2", "0")); //distance
        double m3 = Double.parseDouble(context.getConfiguration().get("current.m3", "0")); //time
        double m4 = Double.parseDouble(context.getConfiguration().get("current.m4", "0")); //tolls
        double b = Double.parseDouble(context.getConfiguration().get("current.b", "0"));
        int iteration = context.getConfiguration().getInt("current.iteration", 0);

        double[] data = new double[6]; // 0: m1, 1: m2, 2: m3, 3: m4, 4: b, 5: cost
        double count = 0;

        for (Wrapper val : values) {
            count++;
            double total_amount = val.getTotalAmount().get();
            double trip_time = val.getTripTime().get();
            double trip_distance = val.getTripDistance().get();
            double fare_amount = val.getFareAmount().get();
            double tolls_amount = val.getTollsAmount().get();

            double[] variables = {trip_time, trip_distance, fare_amount, tolls_amount};
            
            double summation_factor = total_amount - ((m1 * trip_time) + (m2 * trip_distance) + (m3 * fare_amount) + (m4 * tolls_amount) + b);
            for (int i = 0; i < 4; i++){
                data[i] += (-1 * variables[i]) * summation_factor;
            }            

            data[4] += (-1) * summation_factor;
            data[5] += Math.pow(summation_factor, 2);
        }


        double m1part = (2 / count) * data[0]; 
        double m2part = (2 / count) * data[1];
        double m3part = (2 / count) * data[2];
        double m4part = (2 / count) * data[3];
        double bpart = (2 / count) * data[4];

        System.out.println("\n\n\n\n\n\n");
        System.out.println(data[0] + " " + data[1] + " " + data[2] + " " + data[3] + " " + data[4]);
        System.out.println("iteration: " + iteration);
        System.out.println("m1: " + m1 + " m2: " + m2 + " m3: " + m3 + " m4: " + m4 + " b: " + b);
        System.out.println("partials: " + "m: " + m1part + " m2: " + m2part + " m3: " + m3part + " m4: " + m4part + " b: " + bpart);
        System.out.println("Cost: " + data[5]);

        // Use Hadoop Counters to pass the results back
      //   context.getCounter("GradientDescent", "m1part").increment((long)(m1part * 1000000));
      //   context.getCounter("GradientDescent", "m2part").increment((long)(m2part * 1000000));
      //   context.getCounter("GradientDescent", "m3part").increment((long)(m3part * 1000000));
      //   context.getCounter("GradientDescent", "m4part").increment((long)(m4part * 1000000));
      //   context.getCounter("GradientDescent", "bpart").increment((long)(bpart * 1000000));
      //   context.getCounter("GradientDescent", "cost").increment((long)(data[5] * 1000000));

        // Write the results (useful for debugging)
        context.write(new Text("m1"), new DoubleWritable(m1part));
        context.write(new Text("m2"), new DoubleWritable(m2part));
        context.write(new Text("m3"), new DoubleWritable(m3part));
        context.write(new Text("m4"), new DoubleWritable(m4part));
        context.write(new Text("b"), new DoubleWritable(bpart));
   }
}