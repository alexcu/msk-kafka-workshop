package org.demo.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerForReplay {

    private final static Logger logger = LoggerFactory.getLogger(ConsumerForReplay.class.getName());
    private KafkaProducer<String, String> producer;
    private Properties applicationProperties;

    /**
     * reads consumer.properties file from the classpath and builds the application properties instance.
     * @throws IOException
     */
    private void readProperties() throws IOException {
        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String appConfigPath = rootPath + "consumer.properties";
        Properties appProps = new Properties();
        appProps.load(new FileInputStream(appConfigPath));
        this.applicationProperties = appProps;
    }

    /**
     * builds Consumer Configuration properties instance based on the properties specified in the consumer.properties
     * @return java.util.Properties Consumer Configuration object
     * @throws IOException
     */
    private Properties buildConsumerConfig() throws IOException{
        if(this.applicationProperties == null)
            this.readProperties();
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.applicationProperties.getProperty("BOOTSTRAP_SERVERS_CONFIG"));
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.applicationProperties.getProperty("KEY_DESERIALIZER_CLASS_CONFIG"));
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.applicationProperties.getProperty("VALUE_DESERIALIZER_CLASS_CONFIG"));
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,this.applicationProperties.getProperty("GROUP_ID"));
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,this.applicationProperties.getProperty("AUTO_OFFSET_RESET_CONFIG"));
        return consumerProperties;
    }
    /**
     * Creates Kafka Consumer and consume messages from the topic name provided in the consumer.properties file
     * @throws IOException
     */
    private void startConsumer() throws IOException {
        Properties consumerConfig = this.buildConsumerConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerConfig);

        String topic_name = applicationProperties.getProperty("TOPIC");
        int partitionNumber = Integer.valueOf(applicationProperties.getProperty("SEEK_PARTITION_NUMBER")).intValue();
        TopicPartition topicPartition = new TopicPartition(topic_name,partitionNumber);
        long replayFromOffsetNumber = Integer.valueOf(applicationProperties.getProperty("SEEK_OFFSET_POSITION")).longValue();

        consumer.assign(Arrays.asList(topicPartition));
        consumer.seek(topicPartition,replayFromOffsetNumber);

        int numberOfMessagesToBeReplayed = Integer.valueOf(this.applicationProperties.getProperty("NUMBER_OF_MESSAGES_REPLAY")).intValue();

        //This is to ensure we close consumer cleanly
        this.forCleanShutdownOfConsumer(consumer);

        boolean shouldPollForMessages = true;
        // poll for messages
        while(shouldPollForMessages){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(150));
            int counter = 0;
            for (ConsumerRecord<String, String> record : records){
                counter++;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                if(counter == numberOfMessagesToBeReplayed){
                    shouldPollForMessages = false;
                    break;
                }
            }
        }
    }
    public static void main(String[] args) throws IOException {
        ConsumerForReplay replayConsumer = new ConsumerForReplay();
        replayConsumer.startConsumer();
    }
    private void forCleanShutdownOfConsumer(KafkaConsumer<String, String> consumer){
        // Registering a shutdown hook so we can exit cleanly
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("exiting...");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
