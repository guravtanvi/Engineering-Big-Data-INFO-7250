package Question7.b;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<Object, Text, NullWritable, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        context.write(NullWritable.get(),value);
    }
}