package cn.lucifer.hadoop;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.Before;
import org.junit.Test;

public class HdfsTest {
	private Configuration conf;
	FileSystem fs;

	@Before
	public void setUp() throws Exception {
		conf = new Configuration();
		fs = FileSystem.get(conf);
	}

	@Test
	public void testMkdir() throws IOException {
		Path path = new Path("/user/lucifer/hi_mkdir");
		fs.mkdirs(path);
		FileStatus status = fs.getFileStatus(path);
		System.out.println(status.isDir());
	}

	@Test
	public void testCopy() throws IOException {
		Path src = new Path("/home/lucifer/a.txt");
		Path dst = new Path("/user/lucifer/hi_mkdir/a.txt");
		fs.copyToLocalFile(src, dst);
	}

	/**
	 * 列举所有节点
	 * 
	 * @throws IOException
	 */
	@Test
	public void testShowAllNodes() throws IOException {
		// 获取分布式文件系统
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		// 获取所有节点
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

		for (int i = 0; i < dataNodeStats.length; i++) {
			System.out.println(new StringBuilder("DataNode_").append(i).append(
					dataNodeStats[i].getHostName()));
		}
	}

	/**
	 * 查找某個文件中HDFS中的位置
	 * 
	 * @throws IOException
	 */
	@Test
	public void testFindFile() throws IOException {
		Path path = new Path("/home/lucifer/a.txt");
		FileStatus fileStatus = fs.getFileStatus(path);
		BlockLocation[] blkLocations = fs.getFileBlockLocations(fileStatus, 0,
				fileStatus.getLen());
		for (int i = 0, len = blkLocations.length; i < len; i++) {
			String[] hosts = blkLocations[i].getHosts();
			System.out.println(new StringBuilder("block_").append(i)
					.append("_location").append(hosts[0]));
		}
	}

}
