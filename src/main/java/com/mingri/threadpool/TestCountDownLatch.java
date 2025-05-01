package com.mingri.threadpool;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 用来进行线程同步协作，等待所有线程完成倒计时
 **/
public class TestCountDownLatch {

    public static void main(String[] args) throws InterruptedException {
        ExecutorService service  = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(10);
        Random r = new Random();
        String[] all = new String[10];
        for (int i = 0; i < 10; i++) {
            int k = i;
            service.submit(() -> {
                for (int j = 0; j <= 100; j++) {
                    try {
                        Thread.sleep(r.nextInt(100));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    all[k] = j + "%";
                    // "\r" 覆盖上次运行结果
                    System.out.print("\r" + Arrays.toString(all));
                }
                latch.countDown();
            });
        }
        latch.await();
        System.out.println("\n欢迎来到王者荣耀");
        service.shutdown();
    }

}
