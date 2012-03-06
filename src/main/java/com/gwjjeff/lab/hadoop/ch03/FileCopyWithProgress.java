package com.gwjjeff.lab.hadoop.ch03;
// cc FileCopyWithProgress Copies a local file to a Hadoop filesystem, and shows progress
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

// vv FileCopyWithProgress
public class FileCopyWithProgress {
	public static void main(String[] args) throws Exception {
		String localSrc = "/Users/jeff/bench/hadoop/hadoop-book/input/docs/1400-8.txt";
		String dst = "hdfs://localhost/user/jeff/1400-8.txt";
		if (args.length == 2) {
			localSrc = args[0];
			dst = args[1];
		}

		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream out = fs.create(new Path(dst), new Progressable() {
			public void progress() {
				System.out.print(".");
			}
		});

		IOUtils.copyBytes(in, out, 4096, true);
	}
}
// ^^ FileCopyWithProgress
