package Question9;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DriverClass {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "PartitionWindDirection");
        job.setJarByClass(DriverClass.class);

        job.getConfiguration().set("join.type", "inner");
        job.setReducerClass(ReducerClass.class);

        //Partitions the data that is sent to the reducer
        job.setPartitionerClass(PartitionerClass.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        //Used MultipleOutputs as we are taking two data
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperClass.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, WindDirectionClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path outDir = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outDir);

        job.setNumReduceTasks(23);

        FileSystem fs = FileSystem.get(job.getConfiguration());
        if(fs.exists(outDir)){
            fs.delete(outDir, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 2);
    }
}
