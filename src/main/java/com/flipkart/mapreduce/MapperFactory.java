package com.flipkart.mapreduce;

/**
 * Created by dhritiman.das on 4/26/16.
 */
public interface MapperFactory<KeyIn,ValueIn,KeyOut,ValueOut> {

    Mapper<KeyIn,ValueIn,KeyOut,ValueOut> newMapper();
}
