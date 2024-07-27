package org.example;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class LeaderElectionExample extends LeaderSelectorListenerAdapter implements Closeable {
    private final String name;
    private final LeaderSelector leaderSelector;
    private final String filePath;

    public LeaderElectionExample(CuratorFramework client, String path, String name, String filePath) {
        this.name = name;
        this.filePath = filePath;
        leaderSelector = new LeaderSelector(client, path, this);
        leaderSelector.autoRequeue();
    }

    public void start() {
        leaderSelector.start();
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        System.out.println(name + " is now the leader.");
        try {
            while (true) {
                writeToFile();
                TimeUnit.MINUTES.sleep(1);
            }
        } catch (InterruptedException e) {
            System.out.println(name + " was interrupted.");
            Thread.currentThread().interrupt();
        } finally {
            System.out.println(name + " relinquishing leadership.");
        }
    }

    private void writeToFile() {
        try (FileWriter writer = new FileWriter(filePath, true)) {
            writer.write(name + " is the leader at " + System.currentTimeMillis() + "\n");
            System.out.println(name + " wrote to file.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: java LeaderElectionExample <name> <filePath>");
            System.exit(1);
        }

        String name = args[0];
        String filePath = args[1];

        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181", new ExponentialBackoffRetry(1000, 3));
        client.start();

        LeaderElectionExample example = new LeaderElectionExample(client, "/leader", name, filePath);
        example.start();

        // Keep the application running
        Thread.sleep(Long.MAX_VALUE);
    }
}
