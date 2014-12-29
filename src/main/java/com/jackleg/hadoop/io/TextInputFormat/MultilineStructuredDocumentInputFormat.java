package com.jackleg.hadoop.io.TextInputFormat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class MultilineStructuredDocumentInputFormat
	extends FileInputFormat<LongWritable, Text>
	implements JobConfigurable {

	private CompressionCodecFactory compressionCodecs = null;
	
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new MultilineStructuredDocumentRecordReader();
	}

	public void configure(JobConf conf) {
		compressionCodecs = new CompressionCodecFactory(conf);
	}

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return compressionCodecs.getCodec(filename) == null;
	}
}
