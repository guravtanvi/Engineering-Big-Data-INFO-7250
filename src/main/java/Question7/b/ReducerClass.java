package Question7.b;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

public class ReducerClass extends Reducer<NullWritable, Text, NullWritable, Text> {

    static class SortingClass implements Comparator<Integer> {

        @Override
        public int compare(Integer o1, Integer o2) {
            return o2.compareTo(o1);
        }
    }

    private TreeMap<Integer, Text> countTreeMap = new TreeMap<>(new SortingClass());

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {

            String[] tokens = value.toString().split("\t");
            int count = Integer.parseInt(tokens[1]);

            countTreeMap.put(count, new Text(value));

            if (countTreeMap.size() > 5)
                countTreeMap.remove(countTreeMap.lastKey());
        }

        for (Text t : countTreeMap.values())
            context.write(NullWritable.get(), t);
    }
}
