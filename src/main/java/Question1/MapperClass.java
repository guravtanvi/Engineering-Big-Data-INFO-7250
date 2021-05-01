package Question1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

    Text state = new Text();
    LongWritable count = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //Convert the each line which is comma separated into String array
        String line = value.toString();
        String[] tokens = line.split(",");

        //Check if the first column is ID
        if(!tokens[0].equals("ID")) {

            //set column 17 to Text variable
            state.set(tokens[17]);
        }

        //The mapper will emit state key and 1 as count
        context.write(state, count);
    }
}
