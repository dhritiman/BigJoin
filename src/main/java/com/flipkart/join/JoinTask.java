package com.flipkart.join;

import java.util.concurrent.Callable;

/**
 * Created by dhritiman.das on 4/18/16.
 */
public class JoinTask implements Callable<JoinTaskResult> {

    private int start;
    private int batchSize;
    private int numPincodes;
    private JoinData data;
    private String taskId; // for debug

    public JoinTask(int start, int batchSize, int numPincodes, JoinData data)
    {
        this.start =start;
        this.batchSize = batchSize;
        this.numPincodes = numPincodes;
        this.data = data;
        this.taskId = "Task_" + start;
    }
    @Override
    public JoinTaskResult call() throws Exception {
        JoinTaskResult taskResult = data.join(start,batchSize,taskId);
        return taskResult;
    }
}
