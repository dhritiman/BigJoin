package com.flipkart.join;

import java.util.concurrent.*;

/**
 * Created by dhritiman.das on 4/18/16.
 */
public class ListingSplitJoin {

    public static void main(String [] args)
    {
        JoinData joinData = new JoinData();
        System.out.println("Start filling data");
        joinData.fillData();

        long millis = System.currentTimeMillis();
        System.out.println("Start Joining at " + millis);

        //Do threaded join across listings
        // ######

        ExecutorService taskExecutor = Executors.newFixedThreadPool(10);
        CompletionService<JoinTaskResult> joinCompletionService =
                new ExecutorCompletionService<JoinTaskResult>(taskExecutor);

        int numTasks = 20;
        int batchSize = 500000;
        int start = 0;

        for(int i =0 ; i < numTasks; i++)
        {
            joinCompletionService.submit(new JoinTask(start,batchSize,20000,joinData));
            System.out.println("Submitted task " + i);
            start += batchSize;
        }

        for(int tasksHandled=0; tasksHandled< numTasks; tasksHandled++){
            try {
                System.out.println("trying to take from Completion service");
                Future<JoinTaskResult> result = joinCompletionService.take();
                System.out.println("result for a task availble in queue.Trying to get()"  );
                // above call blocks till atleast one task is completed and results availble for it
                // but we dont have to worry which one

                // process the result here by doing result.get()
                JoinTaskResult taskResult = result.get();
                System.out.println("Task " + String.valueOf(tasksHandled) + "Completed - results obtained  " );

            } catch (InterruptedException e) {
                // Something went wrong with a task submitted
                System.out.println("Error Interrupted exception");
                e.printStackTrace();
            } catch (ExecutionException e) {
                // Something went wrong with the result
                e.printStackTrace();
                System.out.println("Error get() threw exception");
            }
        }

        // ######
        long millisAfter = System.currentTimeMillis();
        double timeInSecs = (millisAfter - millis)/1000 ;
        System.out.println("Done with join at " + millisAfter + " - : Time taken = " + timeInSecs );
        System.out.println("Exit");
    }
}
