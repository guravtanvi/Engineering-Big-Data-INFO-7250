package Question3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MapperClass extends Mapper<LongWritable, Text, IntWritable, MinMaxTuple>{

    private final static SimpleDateFormat sdformat = new SimpleDateFormat("yy-MM-dd");
    IntWritable s = new IntWritable();
    MinMaxTuple tuple = new MinMaxTuple();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //Convert the each line which is comma separated into String array
        String line = value.toString();
        String[] tokens  = line.split(",");


        if(!tokens[0].equals("ID")){

            //Checks if the Severity and Visibility are null or empty
            if(tokens[3].isEmpty() || tokens[3].equals(" ") || tokens[27].equals(" ")|| tokens[27].isEmpty()  ) {
                return;
            }

            int severity = Integer.parseInt(tokens[3]);

            //Setting the corresponding visibility in the tuple for each key
            tuple.setMaxVisibility(Float.parseFloat(tokens[27]));
            tuple.setMinVisibility(Float.parseFloat(tokens[27]));
            tuple.setAvgVisibility(Float.parseFloat(tokens[27]));
            tuple.setCount(1);

            try{
                tuple.setMaxStartDate(sdformat.parse(tokens[4]));
            }catch (ParseException e){
                e.printStackTrace();
            }
            s.set(severity);
            context.write(s ,tuple);
        }

    }
}
