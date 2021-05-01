package Question8;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeySortComparator extends WritableComparator {

    protected CompositeKeySortComparator() {
        super(WritableClass.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        WritableClass w1 = (WritableClass) a;
        WritableClass w2 = (WritableClass) b;

        int result = w1.getYear().compareTo(w2.getYear());

        if(result == 0) {
            return w1.getCounty().compareTo(w2.getCounty());
        }

        return result;
    }

}
