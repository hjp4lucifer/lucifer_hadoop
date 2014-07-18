package cn.lucifer.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapClass extends Mapper<Object, Text, Text, IntWritable> {

	private Text keyText = new Text("key");// 当String用
	private IntWritable intValue = new IntWritable(1);// 当int用

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println(key + "-------------" + value.toString());
		// 获取值
		String str = value.toString();

		StringTokenizer stringTokenizer = new StringTokenizer(str);
		while (stringTokenizer.hasMoreTokens()) {
			keyText.set(stringTokenizer.nextToken());
			context.write(keyText, intValue);
		}
	}
}
