package Question8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<WritableClass, IntWritable, Text, IntWritable> {

    Text text = new Text();
    IntWritable count = new IntWritable();

    @Override
    protected void reduce(WritableClass key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for(IntWritable v : values) {
            sum += v.get();
        }
        text.set("County: "+key.getCounty()+ "| Year: " + key.getYear() + " | Accidents: ");

        count.set(sum);
        context.write(text, count);

    }
}
