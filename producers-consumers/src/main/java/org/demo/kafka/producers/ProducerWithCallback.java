package org.demo.kafka.producers;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ProducerWithCallback {

    private final static Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class.getName());
    private Properties applicationProperties;

    /**
     * reads app.properties file from the classpath and builds the application properties instance.
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
     * builds Producer Configuration properties instance based on the properties specified in the app.properties
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
     * Creates Kafka Producer and sends producer record to the topic name provided in the app.properties
     * @throws IOException
     */
    private void startProducer() throws IOException {
        Properties producerConfig = this.buildProducerConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerConfig);
        String topic_name = applicationProperties.getProperty("TOPIC");
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic_name, "just happened event");
        producer.send(record,new ProducerCallback());
        producer.close();
    }
    private void startContinuousProducer() throws IOException {
        Properties producerConfig = this.buildProducerConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerConfig);
        String topic_name = applicationProperties.getProperty("TOPIC");
        int numberOfMesages = Integer.valueOf(applicationProperties.getProperty("NUMBER_OF_MESSAGES")).intValue();
        for(int i = 0;i < numberOfMesages;i++){
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic_name, "just happened event " + i);
            producer.send(record,new ProducerCallback());
        }
        producer.close();
    }
    private class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetaData, Exception e){
            if (e == null) {
                logger.info("Received new metadata. \n" +
                        "Topic:" + recordMetaData.topic() + "\n" +
                        "Partition: " + recordMetaData.partition() + "\n" +
                        "Offset: " + recordMetaData.offset() + "\n" +
                        "Timestamp: " + recordMetaData.timestamp());
            }
            else {
                logger.error("There's been an error from the Producer side");
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws IOException {
        ProducerWithCallback producerWithCallBack = new ProducerWithCallback();
        //producerWithCallBack.startProducer();
        producerWithCallBack.startContinuousProducer();
    }
}
