package br.ufpa;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.LocalTime;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class SyncPrimitive implements Watcher {

    private static final Logger log = LoggerFactory.getLogger(SyncPrimitive.class);
    static ZooKeeper zk = null;
    private static final Object mutex = new Object();

    String root;

    SyncPrimitive(String address) {
        if(zk == null){
            try {
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(address, 3000, this);
                System.out.println("Finished starting ZK: " + zk);
            } catch (IOException e) {
                log.error(e.toString());
                zk = null;
            }
        }
    }

    public void process(WatchedEvent event) {
        synchronized (mutex) {
            if (!event.getType().equals(Event.EventType.None)) {
                System.out.println(LocalTime.now()+": Event - " + event.getType() + " Mutex:" + mutex);
            }
            // Delay in notification to cause deadlock
            var n = new Random().nextInt(20+1);
            try {
                Thread.sleep(n*100);
            } catch (InterruptedException e) {
                log.error(e.toString());
            }
            mutex.notify();
        }
    }

    /**
     * Barrier
     */
    static public class Barrier extends SyncPrimitive {
        int size;
        String name;

        /**
         * Barrier constructor
         */
        Barrier(String address, String root, int size) {
            super(address);
            this.root = root;
            this.size = size;

            // Create barrier node
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
                                  CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    log.error("Keeper exception when instantiating queue: "+ e);
                } catch (InterruptedException e) {
                    log.error("Interrupted exception: " + e);
                }
            }

            // My node name
            try {
                this.name = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException e) {
               log.error(e.toString());
            }
        }

        /**
         * Join barrier. The thread will block here until all processes join the barrier
         */
        boolean enter() throws KeeperException, InterruptedException{
            var createdNode = zk.create(root + "/" + name, new byte[0], Ids.OPEN_ACL_UNSAFE,
                      CreateMode.EPHEMERAL_SEQUENTIAL);
            name = createdNode.split("/")[2];
            System.out.println("Created: "+createdNode);
            while (true) {
                // Key point where current implementation can enter deadlock.
                // Between the znode creation and getChildren, one of the nodes may already be deleted,
                // so list.size() will always be less than size.
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                    if (list.size() < size) {
                        System.out.println(LocalTime.now()+": Waiting to enter the barrier...");
                        mutex.wait();
                    } else {
                        return true;
                    }
                }
            }
        }

        /**
         * Wait until all reach barrier
         */
        boolean leave() throws KeeperException, InterruptedException{
            zk.delete(root + "/" + name, 0);
            System.out.println("Deleted: " + root + "/" + name);
            System.out.println(LocalTime.now()+": Waiting to leave the barrier");
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                    System.out.println(LocalTime.now()+": Remaining in Barrier: "+ list + "\n");

                    if (!list.isEmpty()) {
                        mutex.wait();
                    } else {
                        return true;
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        barrierTest(args);
    }

    public static void barrierTest(String[] args) {
        int size = Integer.parseInt(args[1]);
        Barrier b = new Barrier(args[0], "/b1", size);
        System.out.println("Mutex Id: " + SyncPrimitive.mutex);
        try{
            boolean flag = b.enter();
            System.out.println("ALL PROCESSES ("+args[1]+") JOINED BARRIER");
            if(!flag) System.out.println("Error when entering the barrier");
        } catch (KeeperException | InterruptedException e){
            log.error(e.toString());
        }

        // SIMULATES SOME WORK
        Random rand = new Random();
        int r = rand.nextInt(20);
        System.out.println("WORK WILL TAKE " + (100 * r) / 1000+ "s... I WILL STILL RECEIVE EVENTS BETWEEN TASKS\n");
        for (int i = 0; i < r; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                log.error(e.toString());
            }
        }
        try{
            var flag = b.leave();
            if (!flag) System.out.println("Error when leaving the barrier");
        } catch (KeeperException | InterruptedException e){
            log.error(e.toString());
        }
        System.out.println("Left barrier");
    }
}