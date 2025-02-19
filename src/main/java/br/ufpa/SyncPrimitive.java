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
                System.out.println(LocalTime.now()+": Event - " + event.getType() + " - " + event.getPath());
            }
            // Delay in notification to try to cause deadlock. The higher the delay the higher the chances of deadlock in bad implementations.
            var n = new Random().nextInt(20+1);
            try {
                Thread.sleep(n*100);
            } catch (InterruptedException e) {
                log.error(e.toString());
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
}