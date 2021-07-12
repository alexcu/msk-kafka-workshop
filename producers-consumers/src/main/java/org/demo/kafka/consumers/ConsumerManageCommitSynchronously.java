package org.demo.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerManageCommitSynchronously {
    private final Logger logger = LoggerFactory.getLogger(ConsumerManageCommitSynchronously.class.getName());
    private static final String PROPERTIES_FILE = "app.properties";
    private Properties readConsumerProperties() throws IOException{
        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String appConfigPath = rootPath + ConsumerManageCommitSynchronously.PROPERTIES_FILE;
        Properties appProps = new Properties();
        appProps.load(new FileInputStream(appConfigPath));
        String appVersion = appProps.getProperty("version");
        String name = appProps.getProperty("name");
        logger.info(appVersion);
        logger.info(name);
        return appProps;
    }
    public void startConsumer() throws IOException{
        Properties appProps = this.readConsumerProperties();
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appProps.getProperty("BOOTSTRAP_SERVERS_CONFIG"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, appProps.getProperty("KEY_DESERIALIZER_CLASS_CONFIG"));
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, appProps.getProperty("VALUE_DESERIALIZER_CLASS_CONFIG"));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, appProps.getProperty("GROUP_ID_CONFIG"));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, appProps.getProperty("AUTO_OFFSET_RESET_CONFIG"));

        //your consumer will manage commit itself
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(appProps.getProperty("TOPIC")));
        final Thread mainThread = Thread.currentThread();

       /*
            Consumer polls in an infinite loop, we need to exit cleanly.
            When you decide to exit the poll loop, you will need another thread to call consumer.wakeup()
            If you are running the consumer loop in the main thread, this can be done from ShutdownHook
            Note that consumer.wakeup() is the only consumer method that is safe to call from a different thread
            Calling wakeup will cause poll() to exit with WakeupException,
            or if consumer.wakeup() was called while the thread was not waiting on poll, the exception will be
            thrown on the next iteration when poll() is called.
            The WakeupException doesn’t need to be handled, but before exiting the thread, you must call consumer.close()


            Closing the consumer will commit offsets if needed and will send the group coordinator a message that the consumer
             is leaving the group. The consumer coordinator will trigger rebalancing immediately and you won’t need to wait for
              the session to time out before partitions from the con‐ sumer you are closing will be assigned to another consumer
              in the group
        */

        // Registering a shutdown hook so we can exit cleanly
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Starting exit...");
                // Note that shutdownhook runs in a separate thread, so the only thing we can safely do to a consumer is wake it up
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Received new metadata. \n" +
                            "Topic:" + record.topic() + "\n" +
                            "Partition: " + record.partition() + "\n" +
                            "Offset: " + record.offset() + "\n" +
                            "Key: " + record.key() + "\n" +
                            "Value:" + record.value());
                }
                consumer.commitSync();
            }
        }catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
            System.out.println("Closed consumer and we are done");
        }

    }
    public static void main(String[] args) throws IOException {
        ConsumerManageCommitSynchronously objInstance = new ConsumerManageCommitSynchronously();
        objInstance.startConsumer();
    }
}
