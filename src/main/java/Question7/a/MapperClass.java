package Question7.a;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

    Text weather = new Text();
    LongWritable count = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //Convert the each line which is comma separated into String array
        String line = value.toString();
        String[] tokens = line.split(",");

        //Check if the first column is ID
        if(tokens[0].equals("ID")) {
            weather.set(tokens[31]);
        }
        context.write(weather, count);
    }
}
