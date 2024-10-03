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
        
        double m1 = Double.parseDouble(reader.readLine()); //amount
        double m2 = Double.parseDouble(reader.readLine()); //distance
        double m3 = Double.parseDouble(reader.readLine()); //time
        double m4 = Double.parseDouble(reader.readLine()); //tolls
        double b = Double.parseDouble(reader.readLine());
        int iteration = Integer.parseInt(reader.readLine());
        reader.close();

        double[] data = new double[5]; // 0: m1, 1: m2, 2: m3, 3: m4, 4: b, 5: cost
        double count = 0;

        
        for (Wrapper val : values) {
            count++;
            // System.out.println("Taxi ID: " + text.toString() + " Time: " + val.gettime().get() + " Total: " + val.gettotal().get());
            double distance = val.getDistance().get();
            double fare_amount = val.getFareAmount().get();
            double trip_distance = val.getTripDistance().get();
            double trip_time = val.getTripTime().get();
            double tolls_amount = val.getTollsAmount().get();
            double variables[] = {distance, fare_amount, trip_distance, trip_time, tolls_amount};
            // float multiplied = val.getMultiplied().get();
            double summation_factor = fare_amount - ((m1 * distance) + (m2 * trip_distance) + (m3 * trip_time) + (m4 * tolls_amount) + b);
            for (int i = 0; i < 4; i++){
                data[i] += (-1 * variables[i]) * summation_factor;
            }            

            data[4] += (-1) * (summation_factor);
            data[5] += Math.pow(summation_factor, 2);
        }

         System.out.println(data[0] + " " + data[1] + " " + data[2]);

         double m1part = (2 / count) * data[0]; 
         double m2part = (2 / count) * data[1];
         double m3part = (2 / count) * data[2];
         double m4part = (2 / count) * data[3];
         double bpart = (2 / count) * data[4];

         System.out.println("iteration: " + iteration);
         System.out.println("m1: " + m1 + " m2: " + m2 + " m3: " + m3 + " m4: " + m4 + " b: " + b);
         System.out.println("Cost: " + data[5]);

         context.write(new Text("m1"), new DoubleWritable(m1part));
         context.write(new Text("m2"), new DoubleWritable(m2part));
         context.write(new Text("m3"), new DoubleWritable(m3part));
         context.write(new Text("m4"), new DoubleWritable(m4part));
         context.write(new Text("b"), new DoubleWritable(bpart));
   }
}