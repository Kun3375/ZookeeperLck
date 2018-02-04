package com.kun.zookeeper.service;

import com.kun.zookeeper.util.ZkUtil;

import java.util.concurrent.CountDownLatch;

/**
 * @author CaoZiye
 * @version 1.0 2017/11/23 15:29
 */
public class MyServiceTest {
    
    public static void main(String[] args) throws InterruptedException {
        MyService myService = new MyService();
        CountDownLatch countDownLatch = new CountDownLatch(7);
        
        new Thread(() -> {
            myService.doService("1", 400, false);
            countDownLatch.countDown();
        }).start();
        new Thread(() -> {
            myService.doService("1", 400, false);
            countDownLatch.countDown();
        }).start();
        new Thread(() -> {
            myService.doService("1", 200, true);
            countDownLatch.countDown();
        }).start();
        new Thread(() -> {
            myService.doService("1", 200, true);
            countDownLatch.countDown();
        }).start();
        new Thread(() -> {
            myService.doService("1", 400, false);
            countDownLatch.countDown();
        }).start();
        new Thread(() -> {
            myService.doService("1", 200, true);
            countDownLatch.countDown();
        }).start();
        new Thread(() -> {
            myService.doService("1", 200, false);
            countDownLatch.countDown();
        }).start();
        countDownLatch.await();
        ZkUtil.close();
    }
    
    
}