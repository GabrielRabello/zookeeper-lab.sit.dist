package br.ufpa;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalTime;
import java.util.List;

public class NestedRestrictedBarrier extends SyncPrimitive {
    int size;
    String name;
    final String subsetPath;
    final String readyNodePath;
    String nodePath;
    List<String> barrierLevels;

    /**
     * Barrier constructor
     */
    NestedRestrictedBarrier(String address, String root, String subsetId, int size, List<String> barrierLevels) {
        super(address);
        this.root = root;
        this.size = size;
        this.subsetPath = root + "/" + subsetId;
        this.readyNodePath = this.subsetPath+ "/ready-";
        this.barrierLevels = barrierLevels;

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

    public boolean enter(String barrierLvl) throws KeeperException, InterruptedException {
        var ready = this.readyNodePath + barrierLvl;

        // Step 2: Set watch: exists(b + "/ready", true)
        zk.exists(ready, true);

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
                        if (zk.exists(ready, true) == null) {
                            var readyZNode = zk.create(ready, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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

    public boolean leave(String barrierLvl) throws KeeperException, InterruptedException {
        var ready = this.readyNodePath + barrierLvl;

        System.out.println(LocalTime.now()+": Waiting to leave the barrier");
        var ok = false;
        while (true) {
            synchronized (mutex) {
                // Step 1: L = getChildren(b, false)
                List<String> children = zk.getChildren(subsetPath, false);
                System.out.println(LocalTime.now() + ": Remaining in Barrier: " +
                                           barrierLvl + ": "+children + "\n");
                children.remove("ready-"+barrierLvl);

                // Step 2: if no children, exit
                if (children.isEmpty()) {
                    ok = true;
                    break;
                }

                // Step 3: if p is only process node in L, delete(n) and exit
                if (children.size() == 1 && children.contains(name)) {
                    System.out.println("Deleting barrier node");
                    zk.delete(nodePath, -1);
                    System.out.println("Deleted: " + nodePath);
                    zk.delete(ready, -1);
                    System.out.println("Deleted: " + ready);
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
                    if (zk.exists(nodePath, false) != null) {
                        zk.delete(nodePath, -1);
                        System.out.println("Deleted: " + nodePath);
                    }
                    String lowestNode = children.getFirst();
                    zk.exists(subsetPath + "/"+lowestNode, true);
                    mutex.wait();
                }
            }
        }

        return true;
    }

    public static void main(String[] args) {
        String root = args[0];
        int size = Integer.parseInt(args[1]);
        var subsetId = args[2];
        var barrierLevels = List.of("level1", "level2", "level3"); // Example barrier levels
        var barrier = new NestedRestrictedBarrier(root,  "/b1", subsetId, size, barrierLevels);

        try {
            for (String level : barrierLevels) {
                // Enter the barrier level
                boolean flag = barrier.enter(level);
                System.out.println("ALL PROCESSES (" + size + ") JOINED BARRIER LEVEL \"" + level + "\"");
                if (!flag) System.out.println("Error when entering the barrier level " + level);

                // Perform work for this barrier level
                System.out.println("Performing work for barrier level \"" + level+"\"");
                Worker.doWork();

                // Leave the barrier level
                flag = barrier.leave(level);
                if (!flag) System.out.println("Error when leaving the barrier level " + level);
                System.out.println("Left barrier level \"" + level+"\"\n");
                System.out.println("####################");
            }
        } catch (KeeperException | InterruptedException e) {
            log.error(e.toString());
            System.exit(-1);
        }

        System.out.println("All barrier levels completed");
        barrier.close();
    }
}
