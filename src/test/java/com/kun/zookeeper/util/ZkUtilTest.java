package com.kun.zookeeper.util;

import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Test;

/**
 * @author CaoZiye
 * @version 1.0 2017/11/23 11:19
 */
public class ZkUtilTest {
    
    @Test
    public void testLock() throws Exception{
        ZkUtil.lockAcquire("3375");
        System.out.println("=======\n=======");
    }
    
    @After
    public void tearDown() throws KeeperException, InterruptedException {
        ZkUtil.lockRelease("3375");
    }
    
}
