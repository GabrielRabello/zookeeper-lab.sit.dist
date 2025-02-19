package br.ufpa;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalTime;
import java.util.List;
import java.util.Random;

public class OriginalBarrier extends SyncPrimitive {
    int size;
    String name;

    OriginalBarrier(String address, String root, int size) {
        super(address);
        this.root = root;
        this.size = size;

        // Create barrier node
        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
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

    /**
     * Join barrier. The thread will block here until all processes join the barrier
     */
    boolean enter() throws KeeperException, InterruptedException {
        var createdNode = zk.create(root + "/" + name, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.EPHEMERAL_SEQUENTIAL);
        var split = createdNode.split("/");
        name = split[split.length - 1];
        System.out.println("Created: " + createdNode);
        while (true) {
            // Key point where current implementation can enter deadlock.
            // Between the znode creation and getChildren, one of the nodes may already be deleted,
            // so list.size() will always be less than size.
            synchronized (mutex) {
                List<String> list = zk.getChildren(root, true);
                if (list.size() < size) {
                    System.out.println(LocalTime.now() + ": Waiting to enter the barrier...");
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
    boolean leave() throws KeeperException, InterruptedException {
        zk.delete(root + "/" + name, 0);
        System.out.println("Deleted: " + root + "/" + name);
        System.out.println(LocalTime.now() + ": Waiting to leave the barrier");
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren(root, true);
                System.out.println(LocalTime.now() + ": Remaining in Barrier: " + list + "\n");

                if (!list.isEmpty()) {
                    mutex.wait();
                } else {
                    return true;
                }
            }
        }
    }

    public static void main(String[] args) {
        int size = Integer.parseInt(args[1]);
        var b = new OriginalBarrier(args[0], "/b1", size);
        System.out.println("Mutex Id: " + SyncPrimitive.mutex);
        try {
            boolean flag = b.enter();
            System.out.println("ALL PROCESSES (" + args[1] + ") JOINED BARRIER");
            if (!flag) System.out.println("Error when entering the barrier");
        } catch (KeeperException | InterruptedException e) {
            log.error(e.toString());
        }

        // SIMULATES SOME WORK.
        // THE FASTER THE WORK AND THE LONGER THE NOTIFICATION DELAY, THE HIGHER THE CHANCES OF A DEADLOCK.
        Random rand = new Random();
        int r = rand.nextInt(20);
        System.out.println("WORK WILL TAKE " + (100 * r) / 1000 + "s... I WILL STILL RECEIVE EVENTS BETWEEN TASKS\n");
        for (int i = 0; i < r; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                log.error(e.toString());
            }
        }

        try {
            var flag = b.leave();
            if (!flag) System.out.println("Error when leaving the barrier");
        } catch (KeeperException | InterruptedException e) {
            log.error(e.toString());
        }
        System.out.println("Left barrier");
    }
}
