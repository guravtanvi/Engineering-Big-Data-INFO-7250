package Question11;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //Create a job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BiningBasedOnSides");

        //Binning splits data up in the map phase instead of in the partitioner
        //Hence not needing a reducer
        job.setJarByClass(Driver.class);
        job.setMapperClass(MapperClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //MultipleOutputs class sets up the job’s output to write multiple distinct files
        MultipleOutputs.addNamedOutput(job, "RoadSideBins", TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.setCountersEnabled(job, true);

        job.setNumReduceTasks(0);

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

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
