package Question1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DriverClass {
   public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
       // Create a new Job
       Configuration conf = new Configuration();
       Job job = Job.getInstance(conf, "AccidentsPerState");
       job.setJarByClass(DriverClass.class);

       //Telling the program which is mapper/reducer class
       job.setMapperClass(MapperClass.class);
       job.setReducerClass(ReducerClass.class);

       //Setting the InputFormat (What the data will be like)
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);

       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(LongWritable.class);

       FileSystem fs = FileSystem.get(job.getConfiguration());

       //Passing the file on which word count will be performed
       //Setting the input and output paths
       Path outDir = new Path(args[1]);
       FileOutputFormat.setOutputPath(job, outDir);
       Path inDir = new Path(args[0]);
       FileInputFormat.addInputPath(job, inDir);

       //Delete the output directory if it exists be execution
       if(fs.exists(outDir)){
           fs.delete(outDir, true);
       }

       System.exit(job.waitForCompletion(true) ? 1 : 0);
   }
}
