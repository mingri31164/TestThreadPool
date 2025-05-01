package com.mingri.threadpool;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Thread.sleep;

/**
 * 循环栅栏，用来进行线程协作，等待线程满足某个计数
 * 相较于CountDownLatch在循环事件中需要重复创建CountDownLatch对象
 * CyclicBarrier可以重复使用（每次循环过后计数恢复为初值）
 */
@Slf4j
public class TestCyclicBarrier {

    public static void main(String[] args) {
        ExecutorService service  = Executors.newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2, () -> {
            // 如果线程池大小与CyclicBarrier中的数量不同，则执行结果会有偏差
            // 假如此时线程池数量设为3，则实际应为第一次循环的task1和第二次循环的task1让计数减为0
            // 而不是预期的task1和task2使计数减为0
            // 打印结果会变为task1，task1已完成
            log.debug("task1，task2 已完成");
        });
        for(int i = 0; i < 3; i++){
            service.submit(() -> {
                log.debug("task1开始执行");
                try {
                    sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
            });

            service.submit(() -> {
                log.debug("task2开始执行");
                try {
                    sleep(2);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
            });
        }
        service.shutdown();
    }

}
