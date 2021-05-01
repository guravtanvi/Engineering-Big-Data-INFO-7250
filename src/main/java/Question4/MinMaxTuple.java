package Question4;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class MinMaxTuple implements Writable {

    private Date maxStartDate;

    public Date getMaxStartDate() {
        return maxStartDate;
    }

    public void setMaxStartDate(Date maxStartDate) {
        this.maxStartDate = maxStartDate;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(maxStartDate.getTime());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        maxStartDate =  new Date(dataInput.readLong());
    }

    @Override
    public String toString() {
        return "    Latest Accident Occurred On: " + maxStartDate;
    }
}
