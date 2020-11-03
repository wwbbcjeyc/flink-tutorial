package com.zjtd.other;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
public class SnowflakeThreadTest {
    public static void main(String[] args){
        System.out.println(Snowflake.getNextId(1,2));
        /*long num = 1000;
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for(int i=0;i<num;i++) {
            executorService.execute(new SnowflakeThread());
        }
        executorService.shutdown();*/
    }
}

class SnowflakeThread implements Runnable{
    public void run() {
        System.out.println(Snowflake.getNextId(1,2));
    }
}
