import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CombineUniMapReduce {

    public static class StudentMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(" ");
            String uniProgram = parts[2] + " " + parts[3];
            String name = parts[0] + " " + parts[1];
            context.write(new Text(uniProgram), new Text("S" + name));
        }
    }

    public static class UniversityMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(" ");
            String uniProgram = parts[0] + " " + parts[1];
            context.write(new Text(uniProgram), new Text("U" + parts[2]));
        }
    }

    public static class CombineReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String students = "";
            String units = "";
            for (Text val : values) {
                if (val.toString().startsWith("S")) {
                    students += val.toString().substring(1) + " ";
                } else if (val.toString().startsWith("U")) {
                    units = val.toString().substring(1);
                }
            }
            context.write(key, new Text(students + units));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "combine uni");
        job.setJarByClass(CombineUni.class);
        job.setMapperClass(StudentMapper.class);
        job.setMapperClass(UniversityMapper.class);
        job.setReducerClass(CombineReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}