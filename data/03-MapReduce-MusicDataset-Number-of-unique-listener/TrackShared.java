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

public class TrackShared 
{
	private static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
		{
			String lines = values.toString();
			if(lines.startsWith("UserId"))
			{
				return;
			}
			String[] parts = lines.split("\n");
			for(String words:parts)
			{
				String[] data = words.split(",");
				Text outputKey = new Text(data[1]);
				IntWritable outputValue = new IntWritable(Integer.parseInt(data[2]));
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
			count++;
			context.write(key, new IntWritable(sum));
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			context.write(new Text("The Number of Unique Tracks are"), new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = Job.getInstance(c, "Tracks Shared");
		j.setJarByClass(TrackShared.class);
		j.setMapperClass(MapperClass.class);
		j.setReducerClass(ReducerClass.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}
}