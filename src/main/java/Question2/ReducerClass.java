package Question2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<Text, LongWritable, Text, LongWritable> {

    LongWritable count = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long sum = 0;

        // Iterating through each values and adding
        for(LongWritable val: values){
            sum+=val.get();
        }

        // Converting primitive Long to Hadoop LongWritable
        count.set(sum);

        // Emitting out state and count
        context.write(key, count);
    }
}
