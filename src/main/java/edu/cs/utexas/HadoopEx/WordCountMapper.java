package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

import org.apache.commons.lang.SystemUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
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

				double distance = Double.parseDouble(fields[5]);
				double fare_amount = Double.parseDouble(fields[11]);
				Wrapper wrapper = new Wrapper(distance, fare_amount);
				System.out.println("Mapper: " + wrapper.getDistance() + " " +  wrapper.getFareAmount());
				context.write(new Text("taxi"), wrapper);

		} catch (Exception e) {	
			System.out.println("Error: " + e.getMessage());
			return;
		}
	}
}