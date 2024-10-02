package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Comparator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, Wrapper, Text, DoubleWritable> {

   public void reduce(Text text, Iterable<Wrapper> values, Context context)
           throws IOException, InterruptedException {
	   
        double[] data = new double[5];
        int counter = 0;
        
        for (Wrapper val : values) {
            // System.out.println("Taxi ID: " + text.toString() + " Time: " + val.gettime().get() + " Total: " + val.gettotal().get());
            counter += 1;
            double distance = val.getDistance().get();
            double fare_amount = val.getFareAmount().get();
            // float multiplied = val.getMultiplied().get();
            data[0] += 1;
            data[1] += distance;
            data[2] += fare_amount;
            data[3] += distance * fare_amount;
            data[4] += Math.pow(distance, 2);
        }

        double m = 0;
        double b = 0;
        double n = data[0];
        double sumx = data[1];
        double sumy = data[2];
        double sumxy = data[3];
        double sumx2 = data[4];

        m = ((n * sumxy) - (sumx * sumy)) / ((n * sumx2) - Math.pow(sumx, 2));
        b =  ((sumx2 * sumy) - (sumx * sumxy)) / ((n * sumx2) - Math.pow(sumx, 2));


        context.write(new Text("m"), new DoubleWritable(m));
        context.write(new Text("m"), new DoubleWritable(b));
   }
}