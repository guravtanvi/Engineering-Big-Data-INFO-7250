package Question9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerClass extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text text, Text text2, int i) {
        return text.hashCode() % i;
    }
}
