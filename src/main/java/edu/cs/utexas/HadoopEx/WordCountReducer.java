package edu.cs.utexas.HadoopEx;

import java.io.BufferedReader;
import java.io.FileReader;
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
	   
        // double[] data = new double[5];
        
        // for (Wrapper val : values) {
        //     // System.out.println("Taxi ID: " + text.toString() + " Time: " + val.gettime().get() + " Total: " + val.gettotal().get());
        //     double distance = val.getDistance().get();
        //     double fare_amount = val.getFareAmount().get();
        //     // float multiplied = val.getMultiplied().get();
        //     data[0] += 1;
        //     data[1] += distance;
        //     data[2] += fare_amount;
        //     data[3] += distance * fare_amount;
        //     data[4] += Math.pow(distance, 2);
        // }

        // for(double a : data){
        //     System.out.println("DATA" + a);
        // }

        // double m = 0;
        // double b = 0;
        // double n = data[0];
        // double sumx = data[1];
        // double sumy = data[2];
        // double sumxy = data[3];
        // double sumx2 = data[4];

        // m = ((n * sumxy) - (sumx * sumy)) / ((n * sumx2) - Math.pow(sumx, 2));
        // b =  ((sumx2 * sumy) - (sumx * sumxy)) / ((n * sumx2) - Math.pow(sumx, 2));

        // System.out.println(m);
        // System.out.println(b);

        // context.write(new Text("m"), new DoubleWritable(m));
        // context.write(new Text("b"), new DoubleWritable(b));
        BufferedReader reader = new BufferedReader(new FileReader("params"));
        
        double m = Double.parseDouble(reader.readLine());
        double b = Double.parseDouble(reader.readLine());
        int iteration = Integer.parseInt(reader.readLine());
        reader.close();

        double[] data = new double[3];
        double count = 0;

        
        for (Wrapper val : values) {
            count++;
            // System.out.println("Taxi ID: " + text.toString() + " Time: " + val.gettime().get() + " Total: " + val.gettotal().get());
            double distance = val.getDistance().get();
            double fare_amount = val.getFareAmount().get();
            // float multiplied = val.getMultiplied().get();
            data[0] += (-1 * distance) * (fare_amount - ((m * distance) + b));
            data[1] += -1 * (fare_amount - (m * distance  + b));
            data[2] += Math.pow((fare_amount - (m * distance  + b)), 2);
        }

        System.out.println(data[0] + " " + data[1] + " " + data[2]);

        double mpart = (2 / count) * data[0]; 
        double bpart = (2 / count) * data[1];

        System.out.println("mpart at iteration " + iteration + ": " + mpart);
        System.out.println("bpart at iteration " + iteration + ": " + bpart);
        System.out.println("Cost at iteration " + iteration + ": " + data[2]);

        context.write(new Text("m"), new DoubleWritable(mpart));
        context.write(new Text("b"), new DoubleWritable(bpart));
   }
}