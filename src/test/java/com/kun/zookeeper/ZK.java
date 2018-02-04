package com.kun.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.IOException;

/**
 * @author CaoZiye
 * @version 1.0 2017/9/10 13:24
 */
public class ZK {
    
    private static final Logger logger = Logger.getLogger(ZK.class);
    private static final String ADDRESS = "192.168.188.128";
    private static final int SESSION_TIMEOUT = 2 * 1000;
    private static ZooKeeper zooKeeper;
    
    public String data;
    
    @Before
    public void getZooKeeper(){
        try {
            zooKeeper = new ZooKeeper(ADDRESS, SESSION_TIMEOUT, event -> {
        
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    @After
    public void close(){
        if(zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    @Test
    public void testCreate() throws Exception {
        System.out.println(getNode("/KAKA"));
        Thread.sleep(300000);
    }

    private void createNode(String path,String value){
        try {
            zooKeeper.create(path, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    private String getNode(String path){
        byte[] bytes = null;
        try {
            bytes = zooKeeper.getData(path, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    getNode(path);
                }
            }, new Stat());
            logger.info("data got: " + new String(bytes));
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return data = new String(bytes);
    }
    
}
