package Question10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class ReducerClass extends Reducer<Text, Text, Text, Text> {

    //Random storage of input
    HashSet<String> hs = new HashSet<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        hs.clear();
        StringBuffer sb = new StringBuffer("");

        //Put all cities in hashset
        for(Text v: values){
            hs.add(v.toString());
        }
        //Iterate hashset
        for(String v: hs) {
            sb.append(v);
            sb.append(" , ");
        }

        context.write(key, new Text(sb.toString()));
    }
}
