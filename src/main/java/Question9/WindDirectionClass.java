package Question9;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WindDirectionClass extends Mapper<LongWritable, Text, Text, Text> {

    Text outKey = new Text();
    Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] tokens  = line.split(",");

        //Sets the columns from wind dataset
        String description = tokens[1];
        String windDir = tokens[0];

        outKey.set(windDir);

        //Setting the description as value and # to identify its from second dataset
        outValue.set("#" + description);

        context.write(outKey, outValue);
    }
}

