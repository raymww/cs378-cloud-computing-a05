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

			double lr = 0.001;
			double m = 0;
			double b = 0;
			for(int i = 0; i < 100; i++){
				System.out.println("M at iteration " + i + ": " + m);
				System.out.println("B at iteration " + i + ": " + b);

				BufferedWriter writer = new BufferedWriter(new FileWriter("params", false));
				writer.write(Double.toString(m) + "\n");
				writer.write(Double.toString(b) + "\n");
				writer.write(Integer.toString(i) + "\n");
				writer.close();

				Configuration conf = new Configuration();
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
				FileInputFormat.addInputPath(job, new Path(args[0]));
				job.setInputFormatClass(TextInputFormat.class);

				FileOutputFormat.setOutputPath(job, new Path(args[1] + i));
				job.setOutputFormatClass(TextOutputFormat.class);
				job.waitForCompletion(true);

				BufferedReader reader = new BufferedReader(new FileReader(args[1] + i + "/part-r-00000"));
				
				double mpart = Double.parseDouble(reader.readLine().split("\\s+")[1]);
				double bpart = Double.parseDouble(reader.readLine().split("\\s+")[1]);

				m -= mpart * lr;
				b -= bpart * lr;
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
