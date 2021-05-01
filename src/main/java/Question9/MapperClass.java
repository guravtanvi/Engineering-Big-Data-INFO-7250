package Question9;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

    Text outKey = new Text();
    Text outValue = new Text();
    boolean first = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] tokens  = line.split(",");

        if(tokens[28].equals(" ") || tokens[28].isEmpty() || tokens[0].equals("ID")) {
            return;
        }
        //Setting the wind direction from main dataset
        String windDir = tokens[28];

        //Converting to hadoop datatype
        outKey.set(windDir);

        //Setting the whole tuple as value and @ to identify its the tuple from first dataset
        outValue.set('@' +value.toString());

        context.write(outKey, outValue);
    }
}