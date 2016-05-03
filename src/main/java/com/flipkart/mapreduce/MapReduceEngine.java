package com.flipkart.mapreduce;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by dhritiman.das on 4/25/16.
 */
public class MapReduceEngine<KeyMapIn,ValueInReducer> {

    private int numMappers;
    private int numReducers;
    private MapperFactory mapperFactory;
    private ReducerFactory reducerFactory;
    private ReaderFactory<KeyMapIn> factory;

    //Mappers , Reducers and their necessary objects
    private Map<Integer,MappingTask> mappingTasks;
    private Map<Integer,ReducingTask> reducingTasks;

    //Executors for mappers and reducers
    ExecutorService mappersService;
    ExecutorService reducersService;



    public MapReduceEngine()
    {
        mappingTasks = new HashMap<Integer, MappingTask>();
        reducingTasks = new HashMap<Integer, ReducingTask>();
    }

    public MapReduceEngine mappers(int numMappers)
    {
        this.numMappers = numMappers;
        return this;
    }

    public MapReduceEngine reducers(int numReducers)
    {
        this.numReducers = numReducers;
        return this;
    }

    public MapReduceEngine mapperFactory(MapperFactory mapperFactory)
    {
        this.mapperFactory = mapperFactory;
        return this;
    }

    public MapReduceEngine reducerFactory(ReducerFactory reducerFactory)
    {
        this.reducerFactory = reducerFactory;
        return this;
    }

    public MapReduceEngine readerFactory(ReaderFactory factory)
    {
        this.factory = factory;
        return this;
    }

    public MapReduceEngine init()
    {
        //Create the mapper and reducer services
        mappersService = Executors.newFixedThreadPool( numMappers );
        reducersService = Executors.newFixedThreadPool( numReducers );

        //Create DefaultContext
        Context context = new DefaultContext(numMappers,numReducers);

        //Create MappingTasks
        for(int partitionId = 0 ; partitionId < numMappers; partitionId++)
        {
            PartitionAwareIterator<KeyMapIn> iterator = factory.getReaderForPartition(partitionId);
            MappingTask mappingTask =  new MappingTask(mapperFactory.newMapper(),iterator,context);
            mappingTasks.put(partitionId,mappingTask);
        }

        //Create ReducingTasks
        for(int partitionId = 0 ; partitionId < numReducers; partitionId++)
        {
            ReducingTask<Integer,ValueInReducer> reducingTask =  new ReducingTask(reducerFactory.newReducer(),context,partitionId);
            reducingTasks.put(partitionId,reducingTask);
        }

        return this;
    }

    public MapReduceEngine submit()
    {
        //Submit Mappers
        for( MappingTask mappingTask :mappingTasks.values())
            mappersService.submit(mappingTask);

        //Submit Reducers
        for( ReducingTask reducingTask :reducingTasks.values())
            reducersService.submit(reducingTask);

        //Need to ensure all done by 5 minutes and clean shutdown
        return this;
    }


}
