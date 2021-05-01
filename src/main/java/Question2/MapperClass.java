package Question2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

    Text junction = new Text();
    LongWritable count = new LongWritable(1);
    JunctionClass junctionMap = new JunctionClass();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //Convert the each line which is comma separated into String array
        String line = value.toString();
        String[] tokens = line.split(",");
        String junct = "";

        //Check if the first column is ID
        if(!tokens[0].equals("ID")) {

            //The object of class Junction Class fetches the method that maps true --> Near Junction and false --> Not Near Junction
            junct = junctionMap.getJunctionMethod(tokens[36]);

        }
        //Converting the above mapped object to Text datatype
        junction.set(junct);

        //The mapper will emit state key and 1 as count
        context.write(junction, count);
    }
}
