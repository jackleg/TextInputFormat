package com.jackleg.hadoop.io.TextInputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class MultilineStructuredDocumentRecordReader extends
		RecordReader<LongWritable, Text> {
	
	private CompressionCodecFactory compressionCodecs = null;

	private long start;
	private long pos;
	private long end;
	private LineReader in;
	
	/**
	 * 컬렉션 문서의 시작, 끝 section.
	 */
	private Text endSection;
	
	private final static Text EOL = new Text("\n");
	
	private LongWritable key = new LongWritable();
	private Text value = new Text();

	/**
	 * haystack이 needle로 시작하는지 여부를 판단.
	 * @param haystack
	 * @param needle
	 * @return haystack이 needle로 시작하면 true, 그렇지 않으면 false. 둘 중, 하나라도 null이면 false.
	 */
	public boolean startsWith(Text haystack, Text needle) {
		if(haystack == null || needle == null) return false;
		if(haystack.getLength() < needle.getLength()) return false;
		
		byte[] haystackBytes = haystack.getBytes();
		byte[] needleBytes   = needle.getBytes();
		
		// byte 단위 비교
		for(int i=0;i<needle.getLength();i++) {
			if(haystackBytes[i] != needleBytes[i]) return false;
		}
		
		return true;
	}
	
	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit)genericSplit;
		
		// setup configuration
		Configuration job = context.getConfiguration();
		endSection    = new Text(job.get("com.naver.weblab.collection.end", "#END:"));
		
		// set start & end
		start = split.getStart();
		end = start + split.getLength();
		
		// get file and setup input stream
		Path file = split.getPath();
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(file);
		
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);
		
		// 문서 중간에서 시작되는 경우, 첫 collection은 이미 이전에 처리했으므로 skip한다.
		boolean skipFirstDocument = false;
		if(codec != null) {
			in = new LineReader(codec.createInputStream(fileIn));
			end = Long.MAX_VALUE;
		}
		else {
			if(start != 0) {
				skipFirstDocument = true;
				start--; // start가 EOL에 있을 수도 있으므로, 안전성을 위해 1 byte 앞으로 당김.
				fileIn.seek(start);
			}
			in = new LineReader(fileIn);
		}
		
		if(skipFirstDocument) {
			Text dummy = new Text();
			
			while(startsWith(dummy, endSection) == false) { 
				start += in.readLine(dummy);
			}
		}
		
		pos = start;
	}

	/**
	 * collection을 읽어서 text에 저장.
	 * @param text
	 * @param maxLineLength
	 * @param maxBytesConsumePerLine
	 * @return in stream에서 읽은 byte 수.
	 * @throws IOException 
	 */
	private int readNext(Text text) throws IOException {
		int offset = 0;
		text.clear();
		Text tmp = new Text();
		 
		// 첫번째 라인인지 여부를 판단. 처음에 new line을 추가하지 않기 위함이다.
		boolean isFirstLine = true;
		while(startsWith(tmp, endSection) == false) {
			int offsetTmp = in.readLine(tmp);
			offset += offsetTmp;
			
			if(offsetTmp == 0) break; // EOF

			if(isFirstLine) isFirstLine = false;
			else            text.append(EOL.getBytes(), 0, EOL.getLength());
			
			text.append(tmp.getBytes(), 0, tmp.getLength());
		}
		
		return offset;
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		key.set(pos);
		int newSize = 0;
		
		// end of split
		if(pos < end) {
			newSize = readNext(value);
			pos += newSize;
		}
		
		if(newSize == 0) {
			key = null;
			value = null;
			return false;
		}
		else {
			return true;
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if(start >= end) {
			return 0.0f;
		}
		else {
			return Math.min(1.0f, (pos-start)/(float)(end-start));
		}
	}

	@Override
	public void close() throws IOException {
		if(in != null) in.close();
	}
}
