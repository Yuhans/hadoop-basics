package com.yuhans.bigdata.hadoop.ipinyouparser.writable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * This class is used like Key for MapReduce job.
 * It keeps cityName name for comparing keys and osId which allows us to use this for partitioning.
 *
 * @author Artem_Iushin <Artem_Iushin@epam.com>
 */
public class CityOsWritable implements WritableComparable<CityOsWritable> {

    private Integer cityName;
    private short osId;

    public CityOsWritable() {
    }

    public CityOsWritable(Integer cityName, short osId) {
        this.cityName = cityName;
        this.osId = osId;
    }

    public Integer getCityName() {
        return cityName;
    }

    public void setCityName(Integer cityName) {
        this.cityName = cityName;
    }

    public short getOsId() {
        return osId;
    }

    public void setOsId(short osId) {
        this.osId = osId;
    }

    @Override
    public int compareTo(CityOsWritable o) {
        return cityName.compareTo(o.getCityName());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(cityName);
        out.writeShort(osId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        cityName = in.readInt();
        osId = in.readShort();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CityOsWritable that = (CityOsWritable) o;
        return Objects.equals(cityName, that.cityName) &&
                Objects.equals(osId, that.osId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cityName, osId);
    }
}
