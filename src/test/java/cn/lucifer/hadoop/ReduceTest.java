package cn.lucifer.hadoop;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("all")
public class ReduceTest {

	private Reducer reducer;
	private ReduceDriver driver;

	@Before
	public void setUp() throws Exception {
		reducer = new ReduceClass();
		driver = new ReduceDriver(reducer);
	}

	@Test
	public void test() throws IOException {
		String key = "this";
		List<IntWritable> values = new ArrayList<IntWritable>();
		int sum = 0;
		for (int i = 0; i < 5; i++) {
			sum += i;
			values.add(new IntWritable(i));
		}
		driver.withInput(new Text(key), values);
		driver.withOutput(new Text(key), new IntWritable(sum));
		driver.runTest();
	}

}
