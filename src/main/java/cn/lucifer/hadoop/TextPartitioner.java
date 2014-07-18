package cn.lucifer.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TextPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		return value.toString().hashCode() & Integer.MAX_VALUE % numPartitions;
		// return 0;
	}

}
