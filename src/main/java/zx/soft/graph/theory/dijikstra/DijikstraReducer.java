package zx.soft.graph.theory.dijikstra;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DijikstraReducer extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {

	/*
	 * Overriding the reduce function
	 */
	@Override
	public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output,
			Reporter reporter) throws IOException {
		String nodes = "UNMODED";
		Text word = new Text();
		int lowest = 125; // In this 125 is considered as infinite distance
		while (values.hasNext()) { // looks like NODES/VALUES 1 0 2:3:, we
			// need to use the first as a key
			String[] sp = values.next().toString().split(" "); // splits on
			// space
			// look at first value
			if (sp[0].equalsIgnoreCase("NODES")) {
				nodes = null;
				nodes = sp[1];
			} else if (sp[0].equalsIgnoreCase("VALUE")) {
				int distance = Integer.parseInt(sp[1]);
				lowest = Math.min(distance, lowest);
			}
		}
		word.set(lowest + " " + nodes);
		output.collect(key, word);
		word.clear();
	}

}