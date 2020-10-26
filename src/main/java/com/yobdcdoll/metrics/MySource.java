package com.yobdcdoll.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import org.apache.spark.metrics.source.Source;

import java.io.Serializable;

public class MySource implements Source, Serializable {
    private String sourceName;
    private MetricRegistry metricRegistry = new MetricRegistry();
    private Histogram c = metricRegistry.histogram("aaa.histogram");

    public MySource(String name) {
        this.sourceName = name;
        metricRegistry.register("aaa.map.myMapGram", c);

    }

    @Override
    public String sourceName() {
        return this.sourceName;
    }

    public void inc(long ts) {
        c.update(ts);
    }

    @Override
    public MetricRegistry metricRegistry() {
        return metricRegistry;
    }
}
