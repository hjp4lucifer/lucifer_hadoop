package cn.lucifer.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

/**
 * 自定义一个多文件生成的命名规则
 * 
 * @author Lucifer
 * 
 */
public class AlphabetOutputFormat extends
		MultipleTextOutputFormat<Text, IntWritable> {

	@Override
	protected String generateFileNameForKeyValue(Text key, IntWritable value,
			String name) {
		char c = key.toString().charAt(0);
		c = Character.toLowerCase(c);
		if ('a' <= c && c <= 'z') {
			return c + ".txt";
		}
		return "other.txt";
	}
}
