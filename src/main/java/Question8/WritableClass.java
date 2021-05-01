package Question8;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WritableClass implements WritableComparable<WritableClass> {

    private String county;
    private String year;

    public WritableClass(String state, String year) {
        super();
        this.county = state;
        this.year = year;
    }
    public WritableClass() {
        super();
    }
    public String getCounty() {
        return county;
    }
    public void setCounty(String county) {
        this.county = county;
    }
    public String getYear() {
        return year;
    }
    public void setYear(String year) {
        this.year = year;
    }

    @Override
    public int compareTo(WritableClass o) {
        int result = this.county.compareTo(o.county);
        if(result == 0) {
            return this.year.compareTo(o.year);
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(county);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        county = dataInput.readUTF();
        year = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "county='" + county + '\'' +
                ", year='" + year + '\'';
    }
}