package br.ufpa;

import org.apache.zookeeper.KeeperException;

public interface IBarrier {
    boolean enter() throws KeeperException, InterruptedException;
    boolean leave() throws KeeperException, InterruptedException;
    void close();
}
