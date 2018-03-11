package com.yc.hadoop.hdfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 使用java代码向hdfs中写入文件
 * @company 源辰信息
 * @author navy
 * 使用命令方式
 * 	在hdfs中创建文件夹：bin/hadoop fs -mkdir -p /user/hadoop/input   (无-p也可以)
 * 	上传成功后，可以通过bin下面的  hadoop fs -ls /home/navy/files/*  命令查看上传的文件
 * 	删除文件：hadoop fs -rm -r /home/navy/files/yc.txt
 * 	修改文件的权限： sudo hadoop fs -chmod 777 /user/hadoop
 * 	查看运行结果：sudo hadoop fs cat output/part-r-00000
 */
public class Hadoop_PutFile {
	public static void main(String[] args) {
		try {
			//创建hadoop配置文件对象
			Configuration conf=new Configuration();

			//conf.set("fs.defaultFS", "hdfs://192.168.30.130:9000/");

			//conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());  

			//方法获取FileSystem实例
			FileSystem fs=FileSystem.get(URI.create("hdfs://192.168.30.130:9000/"),conf,"navy"); 

			//创建本地文件数据输入流对象
			InputStream is=new BufferedInputStream(new FileInputStream("yc2.txt"));


			/*
			 * 使用Hadoop的命令put将本地文件README.txt送到HDFS。
			 * hadoop fs -put README.txt  .
			 * 注意上面这个命令最后一个参数是句点（.），这意味着把本地文件放入到默认的工作目录，该命令等价于：
			 * hadoop fs -put README.txt     /user/root
			 */
			Path path=new Path("/input/yc2.txt"); //保存数据输入流所在的hdfs上的文件

			FSDataOutputStream out=fs.create(path); //打开输出到hdfs上的文件输出流对象

			IOUtils.copyBytes(is, out,4096,true); //进行流操作

			System.out.println("创建hadoop分布式文件夹系统对象成功...");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("创建hadoop分布式文件夹系统对象失败...");
		} 
	}
}
