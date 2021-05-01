package Question5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<Text, IntWritable, Text, Text> {

    Text percent = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // The total length of records in the dataset
        int total = 3513617;
        int sum = 0;

        for(IntWritable v : values) {
            sum += v.get();
        }
        //Calculating the percentage
        double percentage = ((double) sum / total) * 100;
        //Formatting into a string
        percent.set(String.format("%.2f", percentage) + "%");

        context.write(key, percent);
    }
}
