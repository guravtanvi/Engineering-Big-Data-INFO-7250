package Question11;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import org.apache.hadoop.io.Text;


public class MapperClass extends Mapper<Object, Text, Text, NullWritable> {

    private MultipleOutputs<Text, NullWritable> multipleOutputs =null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] tokens  = line.split(",");

        if(!tokens[0].equals("ID")) {
            return;
        }
        if(!tokens[14].equalsIgnoreCase("")) {
            String roadSide = tokens[14];
            //bin name, key, value, out put bin name
            if (roadSide.equalsIgnoreCase("L"))
                multipleOutputs.write("RoadSideBins", value, NullWritable.get(), "LeftSide");
            if (roadSide.equalsIgnoreCase("R"))
                multipleOutputs.write("RoadSideBins", value, NullWritable.get(), "RightSide");
        }
    }

    //After mapper it runs and closes the prog
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

}
