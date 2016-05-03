package com.flipkart.mapreduce;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by dhritiman.das on 4/24/16.
 */
public class ReducingTask<KeyIn,ValueIn> implements Runnable {

    private Reducer reducer;
    private Context context;
    private KeyIn key;

    public  ReducingTask(Reducer reducer,
                         Context context,
                         KeyIn key)
    {
        this.reducer = reducer;
        this.context = context;
        this.key = key;
    }

    @Override
    public void run() {

        System.out.println("Starting reducing task .. " + Thread.currentThread());

        //Initialize the reducer
        //The reducer impl provided by client can initialize its data-structures here
        reducer.beginReduce(key);

        //Continue in a infinite loop reading tuples as they are made available by mappers
        while(true)
        {
            ConcurrentLinkedQueue<ValueIn> reducerQueue = context.getQueueForReducer(key);
            //System.out.println("Loop1-2 Redcuer Queue .. " + Thread.currentThread() + " - " +
            //        (reducerQueue != null ?reducerQueue.toString() : "reducerQueue is NULL"));
            if(reducerQueue != null)
            {
                while(!reducerQueue.isEmpty())
                {
                    ValueIn tuple = reducerQueue.poll();
                    reducer.reduce(tuple);
                }
            }
            if( reducerQueue.isEmpty() && context.allMapFinished())
            {
               // System.out.println("Queue is empty and allmapFinished in reducer thread " + Thread.currentThread());
                break;
            }
        }

        //The reducer impl can do its finalization here - like dumping out the resultant data-set
        //cleaning any resources etc.
        reducer.finalizeReduce(key);
        System.out.println("REDUCEREND: Ending reducing task .. " + Thread.currentThread() + " at time " + System.currentTimeMillis());

    }
}
