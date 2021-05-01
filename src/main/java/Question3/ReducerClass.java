package Question3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Date;

public class ReducerClass extends Reducer<IntWritable, MinMaxTuple, IntWritable, MinMaxTuple> {

    MinMaxTuple tuple = new MinMaxTuple();
    @Override
    protected void reduce(IntWritable key, Iterable<MinMaxTuple> values, Context context) throws IOException, InterruptedException {

        float maxVisibility = Float.MIN_VALUE;
        float minVisibility = Float.MAX_VALUE;
        long count = 0;
        float countVisibility = 0;
        Date maxDate = null;
        //Iterating through all the values (MinMaxTuple) for each key
        for(MinMaxTuple t: values){

            if(maxDate == null) {
                maxDate = t.getMaxStartDate();
            }
            countVisibility += t.getAvgVisibility() * t.getCount();
            count += t.getCount();
            if(t.getMinVisibility() < minVisibility) {
                minVisibility = t.getMinVisibility();
            }
            if(t.getMaxVisibility() > maxVisibility) {
                maxVisibility = t.getMaxVisibility();
                maxDate = t.getMaxStartDate();
            }
        }
        //Setting the aggregated values in the tuple
        tuple.setAvgVisibility(countVisibility / count);
        tuple.setMaxVisibility(maxVisibility);
        tuple.setMinVisibility(minVisibility);
        tuple.setMaxStartDate(maxDate);
        tuple.setCount(count);

        context.write(key, tuple);

    }
}
