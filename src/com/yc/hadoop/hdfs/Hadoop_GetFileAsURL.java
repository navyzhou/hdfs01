package com.yc.hadoop.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * 使用java.net.URL访问hdfs，读取指定文件中的内容
 * @company 源辰信息
 * @author navy
 */
public class Hadoop_GetFileAsURL {
	public static void main(String[] args) {
		try {
			//设置支持hdfs文件系统的访问协议
			URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
			
			URL url=new URL("hdfs://192.168.30.130:9000/input/yc2.txt"); //创建url对象
			
			URLConnection con=url.openConnection(); //建立与指定url资源的连接
			
			InputStream is=con.getInputStream();
			
			IOUtils.copyBytes(is, System.out, 4096,true); //使用IOUtils流数据处理工具，把读到的数据流
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
