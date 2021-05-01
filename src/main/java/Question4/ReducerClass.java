package Question4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Date;

public class ReducerClass extends Reducer<Text, MinMaxTuple, Text, MinMaxTuple> {

    MinMaxTuple tuple = new MinMaxTuple();

    @Override
    protected void reduce(Text key, Iterable<MinMaxTuple> values, Context context) throws IOException, InterruptedException {

        Date maxDate = null;

        for(MinMaxTuple t: values){

            if(maxDate == null) {
                maxDate = t.getMaxStartDate();
            }

            //Checking the Max date then the previously set
            if(maxDate.compareTo(t.getMaxStartDate())<=0) {
                maxDate = t.getMaxStartDate();
            }

        }

        tuple.setMaxStartDate(maxDate);
        context.write(key, tuple);

    }
}

