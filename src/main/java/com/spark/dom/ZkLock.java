package com.spark.dom;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
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
    private CountDownLatch latch;

    public ZkLock(String host_name, String lockname){
        try {
            zk = new ZooKeeper(host_name, SESSION_TIME_OUT, this);
            lockName = lockname;
            connectSignal.await();
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
        System.out.println("event: "+ event.toString());
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectSignal.countDown();
        }
        if (event.getType()== Event.EventType.NodeDeleted) { // NO ELSE !!!
            if (latch != null)
                latch.countDown();
        }
    }

    public void lock() {
        System.out.println("Acquire lock<" + lockName + ">");
        try {
            if (tryLock()) {
                System.out.println("Thread " + Thread.currentThread().getId()
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
        System.out.println("--- try to get lock<" + lockName +"> ---");
        try {
            myNode = zk.create(LOCK_ROOT + "/" + lockName + LOCK_TAG,
                    "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(myNode + " created");

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
            System.out.println("minNode: " + minNode);
            if (myNode.equals(minNode)) {
                System.out.println("Minimum znode is mine");
                return true;
            } else {
                String myNodeName = myNode.substring(myNode.lastIndexOf('/') + 1);
                waitNode = LOCK_ROOT + '/' + lockNameNodes.get(Collections.binarySearch(lockNameNodes, myNodeName) - 1);
                System.out.println("Minimum znode is not mine, wait for " + waitNode);
                return false;
            }
        } catch (Exception e) {
            throw new LockException(e);
        }
    }

    private void waitLock() throws InterruptedException, KeeperException, waitLockException {
        //set up a watcher
        System.out.println("--- wait for node<" + waitNode + ">, my node<"+ myNode + "> ---");

        latch = new CountDownLatch(1);
        Stat stat = zk.exists(waitNode, true);

        // Prior node is deleted between execution of trylock and waitlock
        if (stat == null) {
            return;
        }

        for (int i = 0; i < TIMEOUT_TIMES; i++) {
            boolean countToZero = latch.await(LATCH_TIME_OUT, TimeUnit.MILLISECONDS);
            if (countToZero) { // My node is notified by some watcher
                latch = null;
                return;
            }
            stat = zk.exists(waitNode, true);
            // Prior node is deleted somewhere else
            if (stat == null) {
                latch = null;
                return;
            }
        }
        stat = zk.exists(waitNode, true);
        // Prior node is deleted somewhere else
        if (stat == null) {
            latch = null;
            return;
        }
        throw new waitLockException("WaitLock fails!");
    }

    public void unlock() {
        System.out.println("Release lock<" + lockName + ">, my node<"+ myNode + ">");
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









