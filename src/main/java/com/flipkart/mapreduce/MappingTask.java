package com.flipkart.mapreduce;

/**
 * Created by dhritiman.das on 4/24/16.
 */
public class MappingTask implements Runnable {

    private Mapper mapper;
    private PartitionAwareIterator<KeyValue> iterator;
    private Context context;

    public  MappingTask(Mapper mapper,
                        PartitionAwareIterator iterator,
                        Context context)
    {
        this.mapper = mapper;
        this.iterator = iterator;
        this.context = context;
    }

    @Override
    public void run() {

        //Start

        System.out.println("Starting mapping task .. " + Thread.currentThread());

        //read assigned partition of data and call map
        while(iterator.hasNext())
        {
            KeyValue keyValue = iterator.next();
            mapper.map(keyValue.getKey(),keyValue.getValue(), context);
        }

        //End - Notfiy the context that this mapper is finished
        context.mapFinished();

        System.out.println("Ending mapping task .. " + Thread.currentThread());
    }
}
