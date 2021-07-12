package org.demo.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ProducerBasic {
    private final static Logger logger = LoggerFactory.getLogger(ProducerBasic.class.getName());
    private KafkaProducer<String, String> producer;
    private Properties applicationProperties;

    /**
     * reads producer.properties file from the classpath and builds the application properties instance.
     * @throws IOException
     */
    private void readProperties() throws IOException {
        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String appConfigPath = rootPath + "producer.properties";
        Properties appProps = new Properties();
        appProps.load(new FileInputStream(appConfigPath));
        this.applicationProperties = appProps;
    }

    /**
     * builds Producer Configuration properties instance based on the properties specified in the producer.properties
     * @return java.util.Properties Producer Configuration object
     * @throws IOException
     */
    private Properties buildProducerConfig() throws IOException{
        if(this.applicationProperties == null)
            this.readProperties();
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.applicationProperties.getProperty("BOOTSTRAP_SERVERS_CONFIG"));
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.applicationProperties.getProperty("KEY_SERIALIZER_CLASS_CONFIG"));
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.applicationProperties.getProperty("VALUE_SERIALIZER_CLASS_CONFIG"));
        return producerProperties;
    }

    /**
     * Creates Kafka Producer and sends producer record to the topic name provided in the producer.properties
     * @throws IOException
     */
    private void startProducer() throws IOException {
        Properties producerConfig = this.buildProducerConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerConfig);
        String topic_name = applicationProperties.getProperty("TOPIC");
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic_name, "just happened event");
        producer.send(record);
        producer.close();
    }
    public static void main(String[] args) throws IOException {
        ProducerBasic producerBasic = new ProducerBasic();
        producerBasic.startProducer();
    }
}
