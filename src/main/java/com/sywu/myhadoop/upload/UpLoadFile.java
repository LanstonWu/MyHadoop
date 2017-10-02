package com.sywu.myhadoop.upload;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 将本地文件上传到hadoop集群 hdfs 提示权限不足时设置环境变量export HADOOP_USER_NAME=hadoop再运行
 * 
 * @author lanstonwu
 *
 */
public class UpLoadFile {
	public static void main(String[] args) throws IOException {
		// hdfs目录
		String target = "hdfs://192.168.56.12:9000/gsod";
		// 本地文件目录
		File file = new File("/home/lanstonwu/hapood/ncdc/01");
		if (file.exists()) {
			File[] files = file.listFiles();
			if (files.length == 0) {
				System.out.println("文件夹是空的!");
				return;
			} else {
				for (File file2 : files) {// 遍历本地文件目录
					if (file2.isDirectory()) {
						System.out.println("文件夹:" + file2.getAbsolutePath() + "," + file2.getName());
					} else if (file2.getName() != "01") {
						System.out.println("文件:" + file2.getAbsolutePath() + ",name:" + file2.getName());
						// 读取本地文件
						FileInputStream fis = new FileInputStream(new File(file2.getAbsolutePath()));
						Configuration config = new Configuration();
						// Returns the FileSystem for this URI's scheme and authority
						FileSystem fs = FileSystem.get(URI.create(target + "/" + file2.getName()), config);
						// Create an FSDataOutputStream at the indicated Path
						OutputStream os = fs.create(new Path(target + "/" + file2.getName()));
						// 复制数据
						IOUtils.copyBytes(fis, os, 4096, true);
						System.out.println("拷贝完成...");
					}
				}
			}
		} else {
			System.out.println("文件不存在!");
		}
	}
}
