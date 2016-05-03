package com.flipkart.mapreduce;

/**
 * Created by dhritiman.das on 4/24/16.
 */
public interface Mapper<KeyIn,ValueIn,KeyOut,ValueOut> {

    void map(KeyIn key, ValueIn value, Context<KeyOut,ValueOut> context);

}
