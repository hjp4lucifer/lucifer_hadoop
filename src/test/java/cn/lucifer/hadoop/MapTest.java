package cn.lucifer.hadoop;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("all")
public class MapTest {

	private Mapper mapper;
	private MapDriver driver;

	@Before
	public void setUp() throws Exception {
		mapper = new MapClass();
		driver = new MapDriver(mapper);
	}

	@Test
	public void test() throws IOException {
		String line = "Hi my world —— Lucifer !!!";
		IntWritable defaultInt = new IntWritable(1);
		System.out.println(driver);
		driver.withInput(new Text("111"), new Text(line));
		String[] array = line.split(" ");
		for (String str : array) {
			driver.withOutput(new Text(str), defaultInt);
		}
		// driver.withOutput(new Text("abcd"), defaultInt);
		driver.runTest();
	}

}
