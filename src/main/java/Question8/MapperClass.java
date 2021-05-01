package Question8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MapperClass extends Mapper<Object, Text, WritableClass, IntWritable> {

    SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    SimpleDateFormat yearformat = new SimpleDateFormat("yyyy");

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String year = null;
        String line = value.toString();
        String[] tokens = line.split(",");

        if (!tokens[0].equals("ID")) {

            if (tokens[4].isEmpty() || tokens[16].isEmpty() || tokens[4].equals(" ") || tokens[16].equals(" ")) {
                return;
            }

            String county = tokens[16];

            try {
                year = yearformat.format(sdformat.parse(tokens[4]));
            } catch (ParseException e) {
                e.printStackTrace();
            }

            WritableClass writableObj = new WritableClass(county, year);
            context.write(writableObj, new IntWritable(1));
        }
    }
}