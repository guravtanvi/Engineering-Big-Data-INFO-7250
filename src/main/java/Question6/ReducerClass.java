package Question6;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class ReducerClass extends Reducer<NullWritable, Text, NullWritable, Text> {

    static class SortingClass implements Comparator<Integer> {

        @Override
        public int compare(Integer o1, Integer o2) {
            return o2.compareTo(o1);
        }
    }

    //TreeMap save the data of top n
    private TreeMap<Integer, Text> countTreeMap = new TreeMap<>(new SortingClass());

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {

            //We use the previous formed output from Question1 which is in form "State \t Count"
            String line = value.toString();
            String[] tokens  = line.split("\t");

            //Second column is the count
            int count = Integer.parseInt(tokens[1]);

            countTreeMap.put(count, new Text(value));

            if (countTreeMap.size() > 10)
                countTreeMap.remove(countTreeMap.lastKey());
        }

        for (Text t : countTreeMap.values())
            context.write(NullWritable.get(), t);
    }
}