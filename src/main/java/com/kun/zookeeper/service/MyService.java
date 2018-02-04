package com.kun.zookeeper.service;

import com.kun.zookeeper.util.ZkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author CaoZiye
 * @version 1.0 2017/11/22 21:57
 */
public class MyService {
    
    private static final Logger log = LoggerFactory.getLogger(MyService.class);
    private static int balanceA = 1000;
    private static int balanceB = 1000;

    public void doService(String id, int count, boolean operator) {
        
        ZkUtil.lockAcquire(id);
        
        int countUnit = count / 10;
        if (id.equals("1")) {
            if (operator) {
                for (int i = 0; i < countUnit; i++) {
                    balanceA += 10;
                    log.info("thread{}:add...{}", Thread.currentThread().getName(), balanceA);
                }
            } else {
                if (balanceA - count >= 0) {
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                    for (int i = 0; i < countUnit; i++) {
                        log.info("thread{}:sub...{}", Thread.currentThread().getName(), balanceA);
                        balanceA -= 10;
                    }
                } else {
                    log.info("thread:{},余额不足", Thread.currentThread().getName());
                }
            }
            log.info("result...{}", balanceA);
        }
        if (id.equals("2")) {
            if (operator) {
                for (int i = 0; i < countUnit; i++) {
                    log.info("add...{}", balanceB);
                    balanceB += 10;
                }
            } else {
                if (balanceB - count >= 0) {
                    for (int i = 0; i < countUnit; i++) {
                        log.info("sub...{}", balanceB);
                        balanceB -= 10;
                    }
                }
            }
            log.info("result...{}", balanceB);
        }
        
        ZkUtil.lockRelease(id);

    }


}
