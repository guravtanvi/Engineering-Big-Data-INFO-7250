package Question4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MapperClass extends Mapper<LongWritable, Text, Text, MinMaxTuple> {

    private final static SimpleDateFormat sdformat = new SimpleDateFormat("yy-MM-dd");
    Text city = new Text();
    MinMaxTuple tuple = new MinMaxTuple();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //Convert the each line which is comma separated into String array
        String line = value.toString();
        String[] tokens  = line.split(",");

        //Check if the first column is ID
        if(!tokens[0].equals("ID")){

            //Checks if the City and Date are null or empty
            if(tokens[15].isEmpty() || tokens[15].equals(" ") || tokens[4].equals(" ")|| tokens[4].isEmpty() ) {
                return;
            }
            //Setting city
            String c = tokens[15];

            try{
                tuple.setMaxStartDate(sdformat.parse(tokens[4]));
            }catch (ParseException e){
                e.printStackTrace();
            }

            city.set(c);
            context.write(city ,tuple);
        }

    }
}
