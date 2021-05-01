package Question9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;

public class ReducerClass extends Reducer<Text, Text, Text, Text> {

    private ArrayList<Text> list = new ArrayList<Text>();
    private ArrayList<Text> windDirList = new ArrayList<Text>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        list.clear();
        windDirList.clear();

        //Segregates the tuples from each dataset into arraylist based on what it was tagged with.
        for (Text text : values) {
            if (text.charAt(0) == '@') {
                list.add(new Text(text.toString().substring(1)));
            } else if (text.charAt(0) == '#') {
                windDirList.add(new Text(text.toString().substring(1)));
            }
        }

        innerJoinFunction(context);
    }

    public void innerJoinFunction(Context context) throws IOException, InterruptedException {

        String join = context.getConfiguration().get("join.type");

        //Checks the join type from the one set in Driver Class
        if (join.equalsIgnoreCase("inner")) {
            if (!list.isEmpty() && !windDirList.isEmpty()) {

                for (Text dir : windDirList) {

                    for (Text mainTuple : list) {
                        context.write(dir, mainTuple);
                    }
                }
            }
        }
    }


}
