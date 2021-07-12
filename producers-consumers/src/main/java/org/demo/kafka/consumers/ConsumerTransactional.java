package org.demo.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerTransactional {
    private final static Logger logger = LoggerFactory.getLogger(ConsumerTransactional.class.getName());
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
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.applicationProperties.get("BOOTSTRAP_SERVERS_CONFIG"));
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.applicationProperties.get("KEY_DESERIALIZER_CLASS_CONFIG"));
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.applicationProperties.get("VALUE_DESERIALIZER_CLASS_CONFIG"));
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG,this.applicationProperties.get("GROUP_ID"));
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,this.applicationProperties.get("AUTO_OFFSET_RESET_CONFIG"));
        String isolation_level = this.applicationProperties.getProperty("READ_COMMITTED");
        isolation_level = isolation_level != null && isolation_level.equalsIgnoreCase("Y")? "read_committed" : "read_uncommitted";
        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,isolation_level);
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
        consumer.subscribe(Arrays.asList(topic_name));
        //This is to ensure we close consumer cleanly
        this.forCleanShutdownOfConsumer(consumer);
        // poll for messages
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(150));
            for(ConsumerRecord<String,String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
            }
        }
    }
    public static void main(String[] args) throws IOException {
        ConsumerTransactional consumerTransactional = new ConsumerTransactional();
        consumerTransactional.startConsumer();
    }
    private void forCleanShutdownOfConsumer(KafkaConsumer<String, String> consumer){
        // Registering a shutdown hook so we can exit cleanly
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("exiting...");
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
