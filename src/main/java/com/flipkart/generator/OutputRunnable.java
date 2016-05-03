package com.flipkart.generator;

/**
 * Created by dhritiman.das on 4/22/16.
 */
public class OutputRunnable implements Runnable {

    private OutputGenerator outputGenerator;
    private int job;

    public OutputRunnable(OutputGenerator generator, int job)
    {
        this.outputGenerator = generator;
        this.job = job;
    }

    @Override
    public void run() {

        try{
            System.out.println("Starting to write job " + job);
            outputGenerator.writeData();
            System.out.println("Finished writing job " + job);
        }
        catch (Exception ex)
        {
            System.out.println("Error while writing..in out job - " + job++);
        }
    }
}
