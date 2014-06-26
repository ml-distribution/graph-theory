package zx.soft.graph.theory.dijikstra;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/*
 * This class will responsible for taking the input from a file and
 * running the iterative Map-Reduce functionality to perform Dijikstra Algorithm
 *
 */
public class DijikstraDistribute {

	public static String OUT = "outfile";
	public static String IN = "inputlarger";

	/**
	 * 主函数
	 */
	public static void main(String[] args) throws Exception {

		IN = args[0];
		OUT = args[1];
		String input = IN;
		String output = OUT + System.nanoTime();
		boolean isdone = false;

		// Reiteration again and again till the convergence
		while (isdone == false) {
			JobConf conf = new JobConf(DijikstraDistribute.class);
			conf.setJobName("Dijikstra");
			conf.setOutputKeyClass(LongWritable.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(DijikstraMapper.class);
			conf.setReducerClass(DijikstraReducer.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			JobClient.runJob(conf);

			input = output + "/part-00000";
			isdone = true;// set the job to NOT run again!
			Path ofile = new Path(input);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(ofile)));
			HashMap<Integer, Integer> imap = new HashMap<Integer, Integer>();
			String line = br.readLine();
			// Read the current output file and put it into HashMap
			while (line != null) {
				String[] sp = line.split("\t| ");
				int node = Integer.parseInt(sp[0]);
				int distance = Integer.parseInt(sp[1]);
				imap.put(node, distance);
				line = br.readLine();
			}
			br.close();

			// Check for convergence condition if any node is still left then
			// continue else stop
			Iterator<Integer> itr = imap.keySet().iterator();
			while (itr.hasNext()) {
				int key = itr.next();
				int value = imap.get(key);
				if (value >= 125) {
					isdone = false;
				}
			}
			input = output;
			output = OUT + System.nanoTime();
		}
	}

}