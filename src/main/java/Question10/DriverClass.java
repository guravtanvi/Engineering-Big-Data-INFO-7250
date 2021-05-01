package Question10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DriverClass {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //Create a job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "");
        job.setJarByClass(DriverClass.class);

        //Telling the program which is mapper/reducer class
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //Setting the InputFormat (What the data will be like)
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Set number of reducers to 1
        job.setNumReduceTasks(1);

        //Setting the input and output paths
        Path outDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outDir);
        Path inDir = new Path(args[0]);
        FileInputFormat.addInputPath(job, inDir);

        //Delete the output directory if it exists be execution
        FileSystem fs = FileSystem.get(job.getConfiguration());
        if(fs.exists(outDir)){
            fs.delete(outDir, true);
        }
        //wait job to finish and then exists
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
