package zx.soft.graph.theory.dijikstra;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DijikstraMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

	/*
	 * Overriding the map function
	 */
	@Override
	public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter)
			throws IOException {
		Text word = new Text();
		String line = value.toString(); // looks like 1 0 2:3:
		String[] sp = line.split("\t| "); // splits on space
		int distanceadd = Integer.parseInt(sp[1]) + 1;
		String[] PointsTo = sp[2].split(":");
		for (int i = 0; i < PointsTo.length; i++) {
			word.set("VALUE " + distanceadd); // tells me to look at
			// distance value
			output.collect(new LongWritable(Integer.parseInt(PointsTo[i])), word);
			word.clear();
		}

		// pass in current node's distance (if it is the lowest distance)
		word.set("VALUE " + sp[1]);
		output.collect(new LongWritable(Integer.parseInt(sp[0])), word);
		word.clear();

		word.set("NODES " + sp[2]);
		output.collect(new LongWritable(Integer.parseInt(sp[0])), word);
		word.clear();
	}

}
