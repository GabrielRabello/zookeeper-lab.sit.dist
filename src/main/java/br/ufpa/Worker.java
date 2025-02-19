package br.ufpa;

import java.util.Random;

import static br.ufpa.SyncPrimitive.log;

public final class Worker {
    private Worker() {
    }
    public static void doWork() {
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
    }
}
