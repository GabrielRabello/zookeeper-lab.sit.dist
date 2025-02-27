package br.ufpa;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.time.LocalTime;

public class SimpleBarrier extends SyncPrimitive {

    /**
     * Barrier constructor
     */
    SimpleBarrier(String address, String root, boolean starter) {
        super(address);
        this.root = root;

        // Hypothetical condition that unblocks other processes to make their work
        if (starter) {
            System.out.println("Condition met. Creating barrier node...");
            // Create barrier node
            try {
                if (zk.exists(root, true) == null) {
                    zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    System.out.println(LocalTime.now() + ": Barrier node created");
                }
            } catch (KeeperException e) {
                log.error("Keeper exception when instantiating queue: " + e);
            } catch (InterruptedException e) {
                log.error("Interrupted exception: " + e);
            }
            return;
        }

        waitBarrier();
    }

    /**
     * Wait for barrier znode to be removed (if exists) so the process can make some work
     */
    private void waitBarrier() {
        try {
            while (true) {
                synchronized (mutex) {
                    if (zk.exists(root, true) != null) {
                        System.out.println(LocalTime.now() + ": Free to do some work...");
                        return;
                    }
                    System.out.println(LocalTime.now() + ": Waiting for event...");
                    mutex.wait();
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

    public static void main(String[] args) {
        System.out.println("### BARREIRA SIMPLES ###");
        if (args.length < 2) {
            System.out.println("USO: java -jar barreira_simples.jar <IP:Porta> <ehGatilho>");
            System.exit(-1);
        }

        String root = args[0];
        boolean isStarter = args[1].equalsIgnoreCase("true");

        var b = new SimpleBarrier(root, "/b1", isStarter);

        // Perform work
        System.out.println("Performing work...");
        Worker.doWork();

        if (isStarter) {
            b.RemoveBarrier();
        }

        b.close();
    }
}
