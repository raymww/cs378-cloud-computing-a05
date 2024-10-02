package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, Wrapper> {

	// Create a counter and initialize with 1
	private final FloatWritable counter = new FloatWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");
		try {

				String driverID = fields[1].trim();
				word.set(driverID);

				double distance = Float.parseFloat(fields[5]);
				double fare_amount = Float.parseFloat(fields[11]);
				double tolls_amount = Float.parseFloat(fields[15]);
				double time = Float.parseFloat(fields[4]);


				if (tolls_amount < 3 || fare_amount < 3 || fare_amount > 200 || distance < 1 || distance > 50 || time < 120 || time > 3600){
					throw new Exception();
				}

				Wrapper wrapper = new Wrapper(distance, fare_amount);
				context.write(word, wrapper);

		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
			return;
		}
	}
}