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

    protected static final Logger log = LoggerFactory.getLogger(SyncPrimitive.class);
    static ZooKeeper zk = null;
    protected static final Object mutex = new Object();

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

    public void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            log.error(e.toString());
        }
    }

    public static void main(String[] args) {
        String root = args[0];
        int size = Integer.parseInt(args[1]);
        int barrierType = Integer.parseInt(args[2]);
        var subsetId = args[3];
        final var barrierRoot = "/b1";
        switch (barrierType) {
            case 2:
                barrierTest(root, size, new RestrictedBarrier(root, barrierRoot, subsetId, size));
                break;
            default:
                System.out.println("Invalid barrier type");
                System.exit(-1);
        }
    }

    public static void barrierTest(String root, int size, IBarrier b) {
        try{
            boolean flag = b.enter();
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
        b.close();
    }
}