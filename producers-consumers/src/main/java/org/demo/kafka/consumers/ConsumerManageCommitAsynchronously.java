package org.demo.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerManageCommitAsynchronously {
    private final Logger logger = LoggerFactory.getLogger(ConsumerManageCommitAsynchronously.class.getName());
    private static final String PROPERTIES_FILE = "app.properties";
    private Properties readConsumerProperties() throws IOException{
        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String appConfigPath = rootPath + ConsumerManageCommitAsynchronously.PROPERTIES_FILE;
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

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(appProps.getProperty("TOPIC")));
         while(true){
             ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
             for (ConsumerRecord<String,String> record : records){
                 logger.info("Received new metadata. \n" +
                         "Topic:" + record.topic() + "\n" +
                         "Partition: " + record.partition() + "\n" +
                         "Offset: " + record.offset() + "\n" +
                         "Key: " + record.key() + "\n" +
                         "Value:" + record.value());
             }
             consumer.commitAsync();
         }
    }
    public static void main(String[] args) throws IOException {
        ConsumerManageCommitAsynchronously objInstance = new ConsumerManageCommitAsynchronously();
        objInstance.startConsumer();;
    }
}
