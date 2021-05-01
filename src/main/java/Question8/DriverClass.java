package Question8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DriverClass {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // Create a new Job
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "SecondarySorting");
        job.setJarByClass(DriverClass.class);

        job.setGroupingComparatorClass(NaturalKeyGroupComparator.class);  // Group comparator - this takes care of the grouping into the iterable that is received in the reducer
        job.setSortComparatorClass(CompositeKeySortComparator.class);    // SortComparator -- does secondary sorting
        job.setPartitionerClass(PartitionerClass.class);        // Partitioner - this makes sure that the mapper output goes to relevant reducer

        //Telling the program which is mapper/reducer class
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        //Setting the InputFormat (What the data will be like)
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(WritableClass.class);       // for custom class how the reducer has to group
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Setting the input and output paths
        Path outDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outDir);
        Path inDir = new Path(args[0]);
        FileInputFormat.addInputPath(job, inDir);

        //Number of reducers set to 5 -- based on 5 partitions
        job.setNumReduceTasks(5);

        //Delete the output directory if it exists be execution
        FileSystem fs = FileSystem.get(job.getConfiguration());
        if (fs.exists(outDir)) {
            fs.delete(outDir, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
