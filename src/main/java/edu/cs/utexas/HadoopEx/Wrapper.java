package edu.cs.utexas.HadoopEx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class Wrapper implements Writable{
    
    DoubleWritable total_amount;
    DoubleWritable fare_amount;
    DoubleWritable trip_distance;
    DoubleWritable trip_time;
    DoubleWritable tolls_amount;
    // DoubleWritable multiplied;

    public Wrapper() {
        this.total_amount = new DoubleWritable();
        this.fare_amount = new DoubleWritable();
        this.trip_distance = new DoubleWritable();
        this.trip_time = new DoubleWritable();
        this.tolls_amount = new DoubleWritable();
        // this.multiplied = new DoubleWritable();
    }

    public Wrapper(double total_amount, double trip_time, double trip_distance, double fare_amount, double tolls_amount) {
        this.total_amount = new DoubleWritable(total_amount);
        this.trip_time = new DoubleWritable(trip_time);
        this.trip_distance = new DoubleWritable(trip_distance);
        this.fare_amount = new DoubleWritable(fare_amount);
        this.tolls_amount = new DoubleWritable(tolls_amount);
        // this.multiplied = new DoubleWritable(multiplied);

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        total_amount.write(dataOutput);
        trip_time.write(dataOutput);
        trip_distance.write(dataOutput);
        fare_amount.write(dataOutput);
        tolls_amount.write(dataOutput);
        // multiplied.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        total_amount.readFields(dataInput);
        trip_time.readFields(dataInput);
        trip_distance.readFields(dataInput);
        fare_amount.readFields(dataInput);
        tolls_amount.readFields(dataInput);
        // multiplied.readFields(dataInput);
    }
    public DoubleWritable getTotalAmount() {
        return this.total_amount;
    }
    public DoubleWritable getFareAmount() {
        return this.fare_amount;
    }
    public DoubleWritable getTripDistance() {
        return this.trip_distance;
    }
    public DoubleWritable getTripTime() {
        return this.trip_time;
    }
    public DoubleWritable getTollsAmount() {
        return this.tolls_amount;
    }

    // public DoubleWritable getMultiplied() {
    //     return this.multiplied;
    // }

}
