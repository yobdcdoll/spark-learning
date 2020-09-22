package com.yobdcdoll.hive;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class MovieUDAF extends UserDefinedAggregateFunction {
    private StructType inputSchema;
    private StructType bufferSchema;

    public MovieUDAF() {
        List<StructField> inputFields = new ArrayList<>();
        inputFields.add(DataTypes.createStructField(
                "genres"
                , DataTypes.StringType
                , true));
        inputSchema = DataTypes.createStructType(inputFields);

        List<StructField> bufferFields = new ArrayList<>();
        bufferFields.add(DataTypes.createStructField(
                "count"
                , DataTypes.LongType
                , true));
        bufferSchema = DataTypes.createStructType(bufferFields);
    }

    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }

    @Override
    public DataType dataType() {
        return DataTypes.LongType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, 0L);
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        if (!buffer.isNullAt(0)) {
            buffer.update(0, buffer.getLong(0) + 1L);
        }
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        long mergeCount = buffer1.getLong(0) + buffer2.getLong(0);
        buffer1.update(0, mergeCount);
    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer.getLong(0);
    }
}
