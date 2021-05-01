package Question3;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class MinMaxTuple implements Writable {

    private float minVisibility;
    private float maxVisibility;
    private float avgVisibility;
    private long count;
    private Date maxStartDate;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
    public float getMinVisibility() {
        return minVisibility;
    }
    public void setMinVisibility(float minVisibility) {
        this.minVisibility = minVisibility;
    }
    public float getMaxVisibility() {
        return maxVisibility;
    }
    public void setMaxVisibility(float maxVisibility) {
        this.maxVisibility = maxVisibility;
    }
    public float getAvgVisibility() {
        return avgVisibility;
    }
    public void setAvgVisibility(float avgVisibility) {
        this.avgVisibility = avgVisibility;
    }
    public Date getMaxStartDate() {
        return maxStartDate;
    }
    public void setMaxStartDate(Date maxStartDate) {
        this.maxStartDate = maxStartDate;
    }

    //Serialize the fields of this object to out
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(minVisibility);
        dataOutput.writeFloat(maxVisibility);
        dataOutput.writeFloat(avgVisibility);
        dataOutput.writeLong(count);
        dataOutput.writeLong(maxStartDate.getTime());
    }
    //Deserialize the fields of this object from in
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        avgVisibility = dataInput.readFloat();
        minVisibility = dataInput.readFloat();
        maxVisibility = dataInput.readFloat();
        count = dataInput.readLong();
        maxStartDate =  new Date(dataInput.readLong());

    }

    @Override
    public String toString() {
        return  "Minimum Visibility= " + minVisibility +
                " | Maximum Visibility= " + maxVisibility +
                " | Average Visibility= " + avgVisibility +
                " | Maximum Start Date= " + maxStartDate;
    }
}
