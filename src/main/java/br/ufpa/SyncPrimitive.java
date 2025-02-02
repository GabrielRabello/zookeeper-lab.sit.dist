package br.ufpa;
import java.io.IOException;
import java.time.LocalTime;
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
        Barrier(String address, String root, int size, boolean starter) {
            super(address);
            this.root = root;
            this.size = size;

            // Hypothetical condition that unblocks other processes to make their work
            if (starter) {
                System.out.println("Condition met. Creating barrier node...");
                // Create barrier node
                try {
                    if (zk.exists(root, true) == null){
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        System.out.println(LocalTime.now()+": Barrier node created");
                    }
                } catch (KeeperException e) {
                    log.error("Keeper exception when instantiating queue: " + e);
                } catch (InterruptedException e) {
                    log.error("Interrupted exception: " + e);
                }
                return;
            }

            //
            try {
                while (true) {
                    synchronized (mutex) {
                        Stat stat = zk.exists(root, true);
                        if (stat == null) {
                            // Barrier node does not exist, proceed
                            System.out.println(LocalTime.now() + ": No barrier. proceeding...");
                            return;
                        } else {
                            // Barrier node exists, wait for the watch event
                            System.out.println(LocalTime.now() + ": Waiting for barrier node to be removed...");
                            mutex.wait();
                        }
                    }
                }
            } catch (KeeperException e) {
                log.error("Keeper exception when instantiating queue: " + e);
            } catch (InterruptedException e) {
                log.error("Interrupted exception: " + e);
            }
        }

        public void RemoveBarrier() {
            try {
                zk.delete(root, 0);
                System.out.println("Barrier node deleted");
            } catch (InterruptedException | KeeperException e) {
                log.error(e.toString());
            }
        }
    }

    public static void main(String[] args) {
        String root = args[0];
        int size = Integer.parseInt(args[1]); // size not actually used in this example
        boolean isStarter = false;
        if (args.length == 3) {
            isStarter = args[2].equalsIgnoreCase("true");
        }
        barrierTest(root, size, isStarter);
    }

    public static void barrierTest(String root, int size, boolean isStarter) {
        var b = new Barrier(root, "/b1", size, isStarter);

        // SIMULATES SOME WORK
        Random rand = new Random();
        int r = rand.nextInt(100);
        if (isStarter) {
            r = 120;
        }
        System.out.println("WORK WILL TAKE " + (100 * r) / 1000+ "s...\n");
        for (int i = 0; i < r; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                log.error(e.toString());
            }
        }

        System.out.println(LocalTime.now()+": WORK DONE");

        if (isStarter) {
            b.RemoveBarrier();
        }
    }
}