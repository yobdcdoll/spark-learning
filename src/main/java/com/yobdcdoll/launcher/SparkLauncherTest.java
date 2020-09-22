package com.yobdcdoll.launcher;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 通过SparkLauncher提交程序
 */
public class SparkLauncherTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        SparkLauncher launcher = new SparkLauncher()
                .addSparkArg("--master", "yarn")
                .addSparkArg("--deploy-mode", "cluster")
                .addSparkArg("--class", "com.yobdcdoll.rdd.RddTest")
                .setAppResource("/opt/tmp/spark-learning-1.0-SNAPSHOT.jar");

        CountDownLatch countDownLatch = new CountDownLatch(1);
        launcher.startApplication(new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle handle) {
                countDownLatch.countDown();
                System.out.println(handle);
            }

            @Override
            public void infoChanged(SparkAppHandle handle) {
                System.out.println(handle);
            }
        });
        countDownLatch.await();
    }
}
