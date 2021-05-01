package Question10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

    Text city = new Text();
    Text county = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String co = "";
        String ci = "";
        String line = value.toString();
        String[] tokens  = line.split(",");

        if(!tokens[0].equals("ID")) {
            co = tokens[16];
            ci = tokens[15];
        }
        city.set(ci);
        county.set(co);
        context.write(county, city);

    }
}
