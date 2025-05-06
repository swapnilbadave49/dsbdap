import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Max_Logged {
    public static class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd-MM-yyyy HH:mm");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");

            if (parts.length >= 8) {
                String user = parts[1];
                String loginTimeStr = parts[5];
                String logoutTimeStr = parts[7];
                try {
                    Date loginTime = DATE_FORMAT.parse(loginTimeStr);
                    Date logoutTime = DATE_FORMAT.parse(logoutTimeStr);

                    long sessionDuration = logoutTime.getTime() - loginTime.getTime();
                    int sessionDurationMinutes = (int) (sessionDuration / (1000 * 60)); // Convert milliseconds to minutes

                    context.write(new Text(user), new IntWritable(sessionDurationMinutes));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class MaxDurationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable maxDuration = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                int duration = value.get();
                if (duration > max) {
                    max = duration;
                }
            }
            maxDuration.set(max);
            context.write(key, maxDuration);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "max logged");
        job.setJarByClass(Max_Logged.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(MaxDurationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}