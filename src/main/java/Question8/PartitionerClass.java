package Question8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerClass extends Partitioner<WritableClass, IntWritable> {

    @Override
    public int getPartition(WritableClass compositeKeyWritable, IntWritable intWritable, int i) {
        return Integer.parseInt(compositeKeyWritable.getYear()) % i;
    }
}
