package MusicUnique;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Music1 
{
	private static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			if (value.toString().startsWith("UserId")) 
			{
				return;
			}
			String lines = value.toString();
			String parts[] = lines.split("\n");
			for(String words:parts)
			{
				String[]data = words.split(",");
				Text outputKey = new Text(data[0]);
				IntWritable outputValue = new IntWritable(1);
				context.write(outputKey, outputValue);
			}
		}
	}
	
	private static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		int count = 0;
		public void reduce(Text key, Iterable<IntWritable>values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable value:values)
			{
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
			count++;
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			context.write(new Text("The Unique Listeners are"), new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration c = new Configuration();
		String files[] = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = Job.getInstance(c, "Unique Music Listeners");
		j.setJarByClass(Music1.class);
		j.setMapperClass(MapperClass.class);
		j.setReducerClass(ReducerClass.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}
}