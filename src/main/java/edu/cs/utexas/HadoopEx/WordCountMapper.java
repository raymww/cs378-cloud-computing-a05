package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

import org.apache.commons.lang.SystemUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.taskdetails_jsp;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, Wrapper> {

	// Create a counter and initialize with 1
	// private final FloatWritable counter = new FloatWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");
		try {

				String driverID = fields[1].trim();
				word.set(driverID);

				double total_amount = Double.parseDouble(fields[16]);
				double trip_time = Double.parseDouble(fields[4]);
				double trip_distance = Double.parseDouble(fields[5]);
				double fare_amount = Double.parseDouble(fields[11]);
				double tolls_amount = Double.parseDouble(fields[15]);


				// if (tolls_amount > 0){
				// 	System.out.println("Tolls: " + tolls_amount);
				// }


				if (tolls_amount < 3 || fare_amount < 3 || fare_amount > 200 || trip_distance < 1 || trip_distance > 50 || trip_time < 120 || trip_time > 3600){
					// throw new Exception(distance + " " + fare_amount + " " + tolls_amount + " " + time);
					throw new Exception();
				}

				Wrapper wrapper = new Wrapper(total_amount, trip_time, trip_distance, fare_amount, tolls_amount);
				context.write(new Text("taxi"), wrapper);

		} catch (Exception e) {	
			// System.out.println("Error: " + e.getMessage());
			return;
		}
	}
}