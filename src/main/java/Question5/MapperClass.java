package Question5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

        Date date = new Date();
        Text month = new Text();
        IntWritable one = new IntWritable(1);

        SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat monthformat = new SimpleDateFormat("MMMMM");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //Convert the each line which is comma separated into String array
            String line = value.toString();
            String[] tokens  = line.split(",");

            //Check if the first column is ID
            if(tokens[0].equals("ID")) {
                String startDate = tokens[4];

                try {
                    date = sdformat.parse(startDate);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            month.set(monthformat.format(date));
            context.write(month, one);
        }
}