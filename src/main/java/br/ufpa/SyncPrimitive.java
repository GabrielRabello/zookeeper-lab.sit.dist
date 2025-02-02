package br.ufpa;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
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

// This was based on pseudo-code from: https://zookeeper.apache.org/doc/r3.6.3/recipes.html
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
            // Delay in notification to TRY to cause deadlock (will not happen with this implementation)
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
        static final String readyNode = "ready";

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
                    if (zk.exists(root, false) == null) {
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

        public boolean enterDoubleBarrier() throws KeeperException, InterruptedException {
            // Step 2: Set watch: exists(b + "/ready", true)
            zk.exists(root + "/"+readyNode, true);

            // Step 1 and 3: Create a name and child: create(n, EPHEMERAL) (the generated name is using sequential feature)
            var znode = zk.create(root +"/"+name, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("Created: "+znode);
            var split = znode.split("/");
            name = split[split.length-1];

            try {
                while (true) {
                    synchronized (mutex) {
                        // Step 4: L = getChildren(b, false)
                        List<String> children = zk.getChildren(root, false);

                        // Step 5: if fewer children in L than x, wait for watch event
                        if (children.size() < size) {
                            System.out.println(LocalTime.now() + ": Waiting for more processes to enter the barrier...");
                            mutex.wait();
                        } else {
                            // Step 6: else create(b + "/ready", REGULAR). Last process to join barrier creates the ready node
                            if (zk.exists(root + "/"+readyNode, true) == null) {
                                var readyZNode = zk.create(root + "/"+readyNode, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                                System.out.println("Created: "+readyZNode);
                            }
                            return true;
                        }
                    }
                }
            } catch (KeeperException | InterruptedException e) {
                log.error(e.toString());
            }

            return false;
        }

        public boolean leave() throws KeeperException, InterruptedException {
            while (true) {
                synchronized (mutex) {
                    // Step 1: L = getChildren(b, false)
                    List<String> children = zk.getChildren(root, false);
                    System.out.println(LocalTime.now() + ": Remaining in Barrier: " + children + "\n");
                    children.remove(readyNode);

                    // Step 2: if no children, exit
                    if (children.isEmpty()) {
                        return true;
                    }

                    // Step 3: if p is only process node in L, delete(n) and exit
                    if (children.size() == 1) {
                        System.out.println("Deleting barrier node");
                        zk.delete(root+"/"+name, 0);
                        zk.delete(root+"/"+readyNode, 0);
                        return true;
                    }

                    // Step 4: if p is the lowest process node in L, wait on highest process node in L
                    children.sort(String::compareTo);
                    if (children.getFirst().equals(name)) {
                        String highestNode = children.getLast();
                        zk.exists(root + "/" + highestNode, true);
                        mutex.wait();
                    } else {
                        // Step 5: else delete(n) if still exists and wait on lowest process node in L
                        zk.delete(root + "/" + name, 0);
                        System.out.println("Deleted: "+root + "/" + name);
                        String lowestNode = children.getFirst();
                        zk.exists(root + "/" + lowestNode, true);
                        mutex.wait();
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        String root = args[0];
        int size = Integer.parseInt(args[1]);
        barrierTest(root, size);
    }

    public static void barrierTest(String root, int size) {
        Barrier b = new Barrier(root, "/b1", size);
        try{
            boolean flag = b.enterDoubleBarrier();
            System.out.println("ALL PROCESSES ("+size+") JOINED BARRIER");
            if(!flag) System.out.println("Error when entering the barrier");
        } catch (KeeperException | InterruptedException e){
            log.error(e.toString());
            System.exit(-1);
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