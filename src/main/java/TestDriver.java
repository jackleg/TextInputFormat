import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.jackleg.hadoop.io.TextInputFormat.MultilineStructuredDocumentInputFormat;

public class TestDriver
	extends Configured
	implements Tool {

	public static class TestMapper
		extends Mapper<LongWritable, Text, NullWritable, Text> {

		private Text out = new Text();
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			out.set(key + "----------\n" + value);
			context.write(NullWritable.get(), out);
		}
	}
	
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("usage: TestDriver <input path> <output path>");
			System.err.println("");
			return -1;
		}

		Configuration conf = getConf();
		
		Job job = new Job(conf, "[jackleg] test driver");
		job.setJarByClass(TestDriver.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setInputFormatClass(MultilineStructuredDocumentInputFormat.class);
		
		job.setMapperClass(TestMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new TestDriver(), args);
		System.exit(result);
	}
}
