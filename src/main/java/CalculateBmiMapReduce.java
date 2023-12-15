import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CalculateBmiMapReduce {

    public static class BmiMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitValues = value.toString().split(" ");
            double weight = Double.parseDouble(splitValues[3]);
            double height = Double.parseDouble(splitValues[2]);
            double bmi = (weight * 703) / (height * 144 * height);
            if (bmi > 25) {
                word.set(splitValues[0] + " " + splitValues[1]);
                context.write(word, new DoubleWritable(bmi));
            }
        }
    }

    public static class BmiReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "calculate bmi");
        job.setJarByClass(CalculateBmi.class);
        job.setMapperClass(BmiMapper.class);
        job.setCombinerClass(BmiReducer.class);
        job.setReducerClass(BmiReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}