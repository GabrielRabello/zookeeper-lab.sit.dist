package br.ufpa;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalTime;
import java.util.List;

public class RestrictedBarrier extends SyncPrimitive implements IBarrier {
    int size;
    String name;
    final String subsetPath;
    final String readyNodePath;
    String nodePath;

    /**
     * Barrier constructor
     */
    RestrictedBarrier(String address, String root, String subsetId, int size) {
        super(address);
        this.root = root;
        this.size = size;
        this.subsetPath = root + "/" + subsetId;
        this.readyNodePath = this.subsetPath+ "/ready";

        // Create barrier node
        if (zk != null) {
            try {
                if (zk.exists(root, false) == null) {
                    zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                              CreateMode.PERSISTENT);
                }
                if (zk.exists(subsetPath, false) == null) {
                    zk.create(subsetPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
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
        this.nodePath = subsetPath + "/" + name;
    }

    public boolean enter() throws KeeperException, InterruptedException {
        // Step 2: Set watch: exists(b + "/ready", true)
        zk.exists(this.readyNodePath, true);

        // Step 1 and 3: Create a name and child: create(n, EPHEMERAL) (the generated name is using sequential feature)
        var znode = zk.create(nodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Created: "+znode);
        var split = znode.split("/");
        this.name = split[split.length-1];
        this.nodePath = subsetPath + "/" + name;

        try {
            while (true) {
                synchronized (mutex) {
                    // Step 4: L = getChildren(b, false)
                    List<String> children = zk.getChildren(subsetPath, false);

                    // Step 5: if fewer children in L than x, wait for watch event
                    if (children.size() < size) {
                        System.out.println(LocalTime.now() + ": Waiting for more processes to enter the barrier...");
                        mutex.wait();
                    } else {
                        // Step 6: else create(b + "/ready", REGULAR). Last process to join barrier creates the ready node
                        var readyZNode = zk.create(readyNodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        System.out.println("Created: "+readyZNode);

                        return true;
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            if (!(e instanceof KeeperException.NodeExistsException)) {
                log.error(e.toString());
            }
        }

        return false;
    }

    public boolean leave() throws KeeperException, InterruptedException {
        System.out.println(LocalTime.now()+": Waiting to leave the barrier");
        while (true) {
            synchronized (mutex) {
                // Step 1: L = getChildren(b, false)
                List<String> children = zk.getChildren(subsetPath, false);
                System.out.println(LocalTime.now() + ": Remaining in Barrier: " + children + "\n");
                children.remove("ready");

                // Step 2: if no children, exit
                if (children.isEmpty()) {
                    return true;
                }

                // Step 3: if p is only process node in L, delete(n) and exit
                if (children.size() == 1) {
                    System.out.println("Last process remaining...");
                    zk.delete(nodePath, -1);
                    System.out.println("Deleted: " + nodePath);
                    zk.delete(readyNodePath, -1);
                    System.out.println("Deleted: " + readyNodePath);
                    zk.delete(subsetPath, -1);
                    System.out.println("Deleted: " + subsetPath);
                    zk.delete(root, -1);
                    System.out.println("Deleted: " + root);
                    return true;
                }

                // Step 4: if p is the lowest process node in L, wait on highest process node in L
                children.sort(String::compareTo);
                if (children.getFirst().equals(name)) {
                    String highestNode = children.getLast();
                    zk.exists(subsetPath+ "/"+ highestNode, true);
                    mutex.wait();
                } else {
                    // Step 5: else delete(n) if still exists and wait on lowest process node in L
                    zk.delete(nodePath, -1);
                    System.out.println("Deleted: " + nodePath);
                    String lowestNode = children.getFirst();
                    zk.exists(subsetPath + "/"+lowestNode, true);
                    mutex.wait();
                }
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("### BARREIRA RESTRITA ###");
        if (args.length < 3) {
            System.out.println("USO: java -jar barreira_restrita.jar <IP:Porta> <nProcessos> <grupo>");
            System.exit(-1);
        }

        String root = args[0];
        int size = Integer.parseInt(args[1]);
        var subsetId = args[2];
        final var barrierRoot = "/b1";
        var barrier = new RestrictedBarrier(root, barrierRoot, subsetId, size);

        try{
            boolean flag = barrier.enter();
            System.out.println("ALL PROCESSES ("+size+") JOINED BARRIER");
            if(!flag) System.out.println("Error when entering the barrier");
        } catch (KeeperException | InterruptedException e){
            log.error(e.toString());
            System.exit(-1);
        }

        Worker.doWork();

        try{
            var flag = barrier.leave();
            if (!flag) System.out.println("Error when leaving the barrier");
        } catch (KeeperException | InterruptedException e){
            log.error(e.toString());
        }
        System.out.println("Left barrier");
        barrier.close();
    }
}
