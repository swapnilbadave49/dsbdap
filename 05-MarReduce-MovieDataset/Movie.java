import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Movie {

	private static class MapperClass extends Mapper<LongWritable, Text, Text, FloatWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String lines = value.toString();
			if (lines.startsWith("userId")) {
				return;
			}
			String[] parts = lines.split("\n");
			for (String words : parts) {
				String[] data = words.split(",");
				Text outputKey = new Text(data[1]);
				FloatWritable outputvalue = new FloatWritable(Float.parseFloat(data[2]));
				context.write(outputKey, outputvalue);
			}
		}
	}

	private static class ReducerClass extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		Text maxRated = new Text();
		float maxRating = Float.MIN_VALUE;
		private TreeMap<Float, String> mp = new TreeMap<>();

		protected void setup(Context context) throws IOException, InterruptedException {
			context.write(new Text("The Movies with their ID and corresponding averaged ratings are as follows:"),
					null);
		}

		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			float sum = 0;
			float cnt = 0;
			for (FloatWritable value : values) {
				sum += value.get();
				cnt++;
			}

			sum = (sum / cnt);

			mp.put(sum, key.toString());

			if (sum > maxRating) {
				maxRated.set(key);
				maxRating = sum;
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text("Movie With the Highest rating is: "), null);
			context.write(maxRated, new FloatWritable(maxRating));

			int count = 0;

			context.write(new Text("\n\nMovie in sorted order: "), null);
			for (Map.Entry<Float, String> entry : mp.descendingMap().entrySet()) {
				context.write(new Text(entry.getValue()), new FloatWritable(entry.getKey()));
				count++;
				if (count == 10) {
					break;
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = Job.getInstance(c, "Recommend Movie based on Rating");
		j.setJarByClass(Movie.class);
		j.setMapperClass(MapperClass.class);
		j.setReducerClass(ReducerClass.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}
}