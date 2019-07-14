package com.spark.commodity;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class ZkLock implements Watcher {

    private ZooKeeper zk;
    private static final String LOCK_ROOT = "/lock";
    private static final String LOCK_TAG = "_LOCKTAG_";
    private String lockName, myNode, waitNode;
    private static final int SESSION_TIME_OUT = 5000;
    private static final int LATCH_TIME_OUT = 5000;
    private static final int TIMEOUT_TIMES = 10;
    private CountDownLatch connectSignal = new CountDownLatch(1);
    private Map<String, CountDownLatch> nodeLatches = new HashMap<>();
    private Logger logger;

    public ZkLock(String host_name, String lockname, Logger lg){
        try {
            zk = new ZooKeeper(host_name, SESSION_TIME_OUT, this);
            lockName = lockname;
            connectSignal.await();
            logger = lg;
            Stat stat = zk.exists(LOCK_ROOT, false);
            if (stat == null) {
                zk.create(LOCK_ROOT, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            throw new LockException(e);
        }
    }

    public void process(WatchedEvent event) {
        logger.error("event: "+ event.toString());
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectSignal.countDown();
        }
        if (event.getType()== Event.EventType.NodeDeleted) { // NO ELSE !!!
            String deletedNode = event.getPath();

            if (nodeLatches != null) {
                CountDownLatch nodeLatch = nodeLatches.get(deletedNode);
                if (nodeLatch != null) {
                    nodeLatch.countDown();
                }
            }
        }
    }

    public void lock() {
        logger.error("Acquire lock<" + lockName + ">");
        try {
            if (tryLock()) {
                logger.error("Thread " + Thread.currentThread().getId()
                        + " whose node is " + myNode + " get lock<" + lockName +">");
            }
            else {
                waitLock();
            }
        } catch (Exception e) {
            throw new LockException(e);
        }
    }

    private boolean tryLock() {
        logger.error("--- try to get lock<" + lockName +"> ---");
        try {
            myNode = zk.create(LOCK_ROOT + "/" + lockName + LOCK_TAG,
                    "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            logger.error(myNode + " created");

            List<String> lockRootSubNodes = zk.getChildren(LOCK_ROOT, false);
            List<String> lockNameNodes = new LinkedList<>();
            for (String node : lockRootSubNodes) {
                String prefix = node.split(LOCK_TAG)[0];
                if (prefix.equals(lockName)){
                    lockNameNodes.add(node);
                }

            }
            Collections.sort(lockNameNodes);

            // The minimum znode belongs to the thread which will get the lock in the next turn
            String minNode = LOCK_ROOT + '/' + lockNameNodes.get(0);
            logger.error("minNode: " + minNode);
            if (myNode.equals(minNode)) {
                logger.error("Minimum znode is mine");
                return true;
            } else {
                String myNodeName = myNode.substring(myNode.lastIndexOf('/') + 1);
                waitNode = LOCK_ROOT + '/' + lockNameNodes.get(Collections.binarySearch(lockNameNodes, myNodeName) - 1);
                logger.error("Minimum znode is not mine, wait for " + waitNode);
                return false;
            }
        } catch (Exception e) {
            throw new LockException(e);
        }
    }

    private void waitLock() throws InterruptedException, KeeperException, waitLockException {
        //set up a watcher
        logger.error("--- wait for node<" + waitNode + ">, my node<"+ myNode + "> ---");

        CountDownLatch nodeLatch = new CountDownLatch(1);
        nodeLatches.put(waitNode, nodeLatch);

        Stat stat = zk.exists(waitNode, true);

        // Prior node is deleted between execution of trylock and waitlock
        if (stat == null) {
            nodeLatches.remove(waitNode);
            return;
        }

        for (int i = 0; i < TIMEOUT_TIMES; i++) {
            boolean countToZero = nodeLatch.await(LATCH_TIME_OUT, TimeUnit.MILLISECONDS);
            if (countToZero) { // My node is notified by some watcher
                nodeLatch = null;
                nodeLatches.remove(waitNode);
                return;
            }
            stat = zk.exists(waitNode, true);
            // Prior node is deleted somewhere else
            if (stat == null) {
                nodeLatch = null;
                nodeLatches.remove(waitNode);
                return;
            }
        }
        stat = zk.exists(waitNode, false);
        // Prior node is deleted somewhere else
        if (stat == null) {
            nodeLatch = null;
            nodeLatches.remove(waitNode);
            return;
        }
        nodeLatches.remove(waitNode);
        throw new waitLockException("WaitLock fails!");
    }

    public void unlock() {
        logger.error("Release lock<" + lockName + ">, my node<"+ myNode + ">");
        try {
            zk.delete(myNode, -1);
            myNode = null;
            zk.close();
        } catch (Exception e) {
            throw new LockException(e);
        }
    }

    public class LockException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public LockException(String e){
            super(e);
        }
        public LockException(Exception e){
            super(e);
        }
    }

    public class waitLockException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public waitLockException(String e){
            super(e);
        }
        public waitLockException(Exception e){
            super(e);
        }
    }

}
