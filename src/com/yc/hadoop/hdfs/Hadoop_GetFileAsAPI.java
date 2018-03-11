package com.yc.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 使用hadoop api访问hdfs，读取指定文件中的内容
 * @company 源辰信息
 * @author navy
 */
public class Hadoop_GetFileAsAPI {
	public static void main(String[] args) {
		try {
			Configuration conf=new Configuration(); //创建hadoop配置文件对象
			
			FileSystem fs=FileSystem.get(URI.create("hdfs://192.168.30.130:9000/"),conf,"navy");
			
			Path file=new Path("/input/yc2.txt");//创建hadoop分布式文件夹系统上的文件对象
			
			FSDataInputStream fsdis=fs.open(file);//打开文件获取文件中的数据流
			
			IOUtils.copyBytes(fsdis, System.out, 4096,true); //使用IOUtils流数据处理工具，把读到的数据流
		}  catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
