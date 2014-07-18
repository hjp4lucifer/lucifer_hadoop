package cn.lucifer.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("all")
public class MapReduceTest {

	private Mapper mapper;
	private Reducer reducer;
	private MapReduceDriver driver;

	@Before
	public void setUp() throws Exception {
		mapper = new MapClass();
		reducer = new ReduceClass();
		driver = new MapReduceDriver(mapper, reducer);
	}

	@Test
	public void test() throws IOException {
		String line = "Hi my world —— Lucifer !!!";
		IntWritable defaultInt = new IntWritable(1);
		driver.withInput(new Text("111"), new Text(line));
		String[] array = line.split(" ");

		// driver.withOutput(new Text("Hi"), defaultInt);
		Arrays.sort(array);// 排序, 因为输出结果是有序的

		for (String str : array) {
			System.out.println("output : " + str);
			driver.withOutput(new Text(str), defaultInt);
		}
		// List list = driver.run();
		// System.out.println("-----------------------------");
		// for (Object li : list) {
		// System.out.println(li);
		// }
		driver.runTest();

	}

	public void mainSimple() throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] args = new String[] {};
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage : wordcount <in> <out> ");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(this.getClass());
		job.setMapperClass(MapClass.class);
		// job.setCombinerClass(IntSumReducer.class);
		// job.setNumReduceTasks(3);//就是TextPartitioner中传入的numPartitions
		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
