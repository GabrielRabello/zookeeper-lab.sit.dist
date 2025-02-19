package br.ufpa;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalTime;
import java.util.List;

public class DoubleBarrier extends SyncPrimitive {
    int size;
    String name;
    static final String readyNode = "ready";

    /**
     * Barrier constructor
     */
    DoubleBarrier(String address, String root, int size) {
        super(address);
        this.root = root;
        this.size = size;

        // Create barrier node
        if (zk != null) {
            try {
                if (zk.exists(root, false) == null) {
                    zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                              CreateMode.PERSISTENT);
                }
            } catch (KeeperException e) {
                log.error("Keeper exception when instantiating queue: " + e);
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

    public boolean enter() throws KeeperException, InterruptedException {
        // Step 2: Set watch: exists(b + "/ready", true)
        zk.exists(root + "/" + readyNode, true);

        // Step 1 and 3: Create a name and child: create(n, EPHEMERAL) (the generated name is using sequential feature)
        var znode = zk.create(root + "/" + name, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Created: " + znode);
        var split = znode.split("/");
        name = split[split.length - 1];

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
                        if (zk.exists(root + "/" + readyNode, true) == null) {
                            var readyZNode = zk.create(root + "/" + readyNode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                            System.out.println("Created: " + readyZNode);
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
        System.out.println(LocalTime.now() + ": Waiting to leave the barrier");
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
                    zk.delete(root + "/" + name, -1);
                    // deleting the ready node guarantees that the next barrier will be created
                    zk.delete(root + "/" + readyNode, -1);
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
                    zk.delete(root + "/" + name, -1);
                    System.out.println("Deleted: " + root + "/" + name);
                    String lowestNode = children.getFirst();
                    zk.exists(root + "/" + lowestNode, true);
                    mutex.wait();
                }
            }
        }
    }

    public static void main(String[] args) {
        String root = args[0];
        int size = Integer.parseInt(args[1]);
        var b = new DoubleBarrier(root, "/b1", size);
        try {
            boolean flag = b.enter();
            System.out.println("ALL PROCESSES (" + size + ") JOINED BARRIER");
            if (!flag) System.out.println("Error when entering the barrier");
        } catch (KeeperException | InterruptedException e) {
            log.error(e.toString());
            System.exit(-1);
        }

        Worker.doWork();

        try {
            var flag = b.leave();
            if (!flag) System.out.println("Error when leaving the barrier");
        } catch (KeeperException | InterruptedException e) {
            log.error(e.toString());
        }
        System.out.println("Left barrier");
        b.close();
    }
}

