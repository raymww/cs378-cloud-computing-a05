package edu.cs.utexas.HadoopEx;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(res);
	}

	/**
	 * 
	 */
	public int run(String args[]) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(args[0]));
            BufferedWriter writer = new BufferedWriter(new FileWriter("cleaned_dataset.csv", false));
            String line;

            // Process each line from the input CSV file
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(","); // Split by comma


				try {
					String driverID = fields[1].trim();
					double distance = Double.parseDouble(fields[5]);
					double fare_amount = Double.parseDouble(fields[11]);
					double tolls_amount = Double.parseDouble(fields[15]);
					double time = Double.parseDouble(fields[4]);

					if (tolls_amount < 3 || fare_amount < 3 || fare_amount > 200 || distance < 1 || distance > 50 || time < 120 || time > 3600){
						// throw new Exception(distance + " " + fare_amount + " " + tolls_amount + " " + time);
						throw new Exception();
					} 
					writer.write(line + "\n");
				} catch (Exception e) {
					continue;
				}
            }

			reader.close();
			writer.close();

			double m = 1;
			double b = 0;
			double lr = 0.001;


			for (int i = 0; i < 100; i ++){
				System.out.println("M at iteration " + i + ": " + m);
				System.out.println("B at iteration " + i + ": " + b);
				Configuration conf = new Configuration();
				conf.set("m", Double.toString(m));
				conf.set("b", Double.toString(b));
				conf.setInt("iteration", i);

				Job job = new Job(conf, "WordCount");
				job.setJarByClass(WordCount.class);

				// specify a Mapper
				job.setMapperClass(WordCountMapper.class);

				// specify a Reducer
				job.setReducerClass(WordCountReducer.class);

				// specify output types
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Wrapper.class);

				// specify input and output directories
				FileInputFormat.addInputPath(job, new Path("cleaned_dataset.csv"));
				job.setInputFormatClass(TextInputFormat.class);

				FileOutputFormat.setOutputPath(job, new Path(args[1] + i));
				job.setOutputFormatClass(TextOutputFormat.class);
				job.waitForCompletion(true);
				
				reader = new BufferedReader(new FileReader(args[1] + "/" + i));
				Double mpartial = Double.parseDouble(reader.readLine().split(" ")[1]);
				Double bpartial = Double.parseDouble(reader.readLine().split(" ")[1]);
				
				m += mpartial * lr;
				b += bpartial * lr;

			}

			System.out.println("Final M: " + m);
			System.out.println("Final B: " + b);
			

			// if (!job.waitForCompletion(true)) {
			// 	return 1;
			// }

			// Job job2 = new Job(conf, "TopK");
			// job2.setJarByClass(WordCount.class);

			// // specify a Mapper
			// job2.setMapperClass(TopKMapper.class);

			// // specify a Reducer
			// job2.setReducerClass(TopKReducer.class);

			// // specify output types
			// job2.setOutputKeyClass(Text.class);
			// job2.setOutputValueClass(FloatWritable.class);

			// // set the number of reducer to 1
			// job2.setNumReduceTasks(1);

			// // specify input and output directories
			// FileInputFormat.addInputPath(job2, new Path(args[1]));
			// job2.setInputFormatClass(KeyValueTextInputFormat.class);

			// FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			// job2.setOutputFormatClass(TextOutputFormat.class);
			return 0;

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
	}
}
