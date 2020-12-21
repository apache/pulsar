package org.apache.pulsar.client;

import java.util.concurrent.CompletableFuture;

public class Test {

    public static void main(String[] args) throws InterruptedException {
        CompletableFuture<Void> completableFuture =  CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        });

        Test test = new Test();
        test.test4(completableFuture);

        Thread.sleep(1000 * 5);
    }

    public void test4(CompletableFuture<Void> completableFuture) {
        completableFuture.thenAccept(ignored -> {
            System.out.println(1);
        });

        completableFuture.thenAccept(ignored -> {
            System.out.println(2);
        });

        completableFuture.thenAccept(ignored -> {
            System.out.println(3);
        });

        completableFuture.thenAccept(ignored -> {
            System.out.println(4);
        });
    }

    public void test3(CompletableFuture<Void> completableFuture) {
        completableFuture.thenCompose(ignored -> {
            System.out.println(1);
            return null;
        });
        completableFuture.thenCompose(ignored -> {
            System.out.println(2);
            return null;
        });
        completableFuture.thenCompose(ignored -> {
            System.out.println(3);
            return null;
        });
        completableFuture.thenCompose(ignored -> {
            System.out.println(4);
            return null;
        });
    }

    public void test2(CompletableFuture<Void> completableFuture) {
        completableFuture.whenComplete((ignored, t) -> {
            System.out.println(1);
        });
        completableFuture.whenComplete((ignored, t) -> {
            System.out.println(2);
        });
        completableFuture.whenComplete((ignored, t) -> {
            System.out.println(3);
        });
    }

    public void test1(CompletableFuture<Void> completableFuture) {
        CompletableFuture<Void> preFuture = completableFuture.thenCompose(ignored -> {
            System.out.println(1);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return CompletableFuture.completedFuture(null);
        });

        CompletableFuture<Void> preFuture2 = preFuture.thenCompose(ignored -> {
            System.out.println(2);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return CompletableFuture.completedFuture(null);
        });

        CompletableFuture<Void> preFuture3 = preFuture.thenCompose(ignored -> {
            System.out.println(3);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return CompletableFuture.completedFuture(null);
        });

        CompletableFuture<Void> preFuture4 = preFuture.thenCompose(ignored -> {
            System.out.println(4);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return CompletableFuture.completedFuture(null);
        });
    }

}
