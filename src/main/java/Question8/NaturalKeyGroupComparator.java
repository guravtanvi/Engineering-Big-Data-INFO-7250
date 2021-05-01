package Question8;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupComparator extends WritableComparator {

    public NaturalKeyGroupComparator() {
        super(WritableClass.class, true);
    }

    //Which reducer to send
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WritableClass w1 = (WritableClass) a;
        WritableClass w2 = (WritableClass) b;

        return w1.getCounty().compareTo(w2.getCounty());

    }
}
