package org.demo.kafka.producers;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class TransactionalProducer {

    private final static Logger logger = LoggerFactory.getLogger(TransactionalProducer.class.getName());
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
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.applicationProperties.getProperty("BOOTSTRAP_SERVERS_CONFIG"));
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.applicationProperties.getProperty("KEY_SERIALIZER_CLASS_CONFIG_INTEGER"));
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.applicationProperties.getProperty("VALUE_SERIALIZER_CLASS_CONFIG"));
        //mandatory settings for Transactional producer
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, this.applicationProperties.getProperty("TRANSACTION_ID"));
        producerProperties.put(ProducerConfig.ACKS_CONFIG,this.applicationProperties.get("NUMBER_OF_ACKS"));
        return producerProperties;
    }

    /**
     * Creates Kafka Producer and sends producer record to the topic name provided in the app.properties
     * @throws IOException
     */
    private void startProducer() throws IOException {
        Properties producerConfig = this.buildProducerConfig();
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(producerConfig);
        String topic_name = applicationProperties.getProperty("TOPIC");
        producer.initTransactions();
        logger.info("Starting First Transaction...");
        //starting a transaction context
        producer.beginTransaction();
        try {
            ProducerRecord<Integer, String> propertyEvent_1 = new ProducerRecord<Integer, String>(topic_name, 1, "property event message-T1-1");
            ProducerRecord<Integer, String> listingEvent_1 = new ProducerRecord<Integer, String>(topic_name, 2, "listing event message-T1-2");
            producer.send(propertyEvent_1, new TransactionalProducerCallback());
            producer.send(listingEvent_1, new TransactionalProducerCallback());
            logger.info("Committing First Transaction.");
            producer.commitTransaction();
        } catch (Exception e) {
            logger.error("Exception in First Transaction. Aborting...");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }
        logger.info("Starting Second Transaction...");
        producer.beginTransaction();
        try {
            ProducerRecord<Integer, String> propertyEvent_2 = new ProducerRecord<Integer, String>(topic_name, 1, "property event message-T2-1");
            ProducerRecord<Integer, String> listingEvent_2 = new ProducerRecord<Integer, String>(topic_name, 2, "listing event message-T2-2");
            logger.info("Writing first message from Transaction-2");
            producer.send(propertyEvent_2, new TransactionalProducerCallback());
            producer.flush();
            Thread.sleep(1000);
            producer.send(listingEvent_2, new TransactionalProducerCallback());
            logger.info("Aborting Second Transaction.");
            //producer.commitTransaction();
            producer.abortTransaction();
        } catch (Exception e) {
            logger.error("Exception in Second Transaction. Aborting...");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }
        logger.info("Finished - Closing Kafka Producer.");
        producer.close();
    }
    private class TransactionalProducerCallback implements Callback{
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e == null) {
                logger.info("Received new metadata. \n" +
                        "Topic:" + recordMetadata.topic() + "\n" +
                        "Partition: " + recordMetadata.partition() + "\n" +
                        "Offset: " + recordMetadata.offset() + "\n" +
                        "Timestamp: " + recordMetadata.timestamp());
            }
            else {
                logger.error("There's been an error from the Producer side");
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws IOException {
        TransactionalProducer transactionalProducer = new TransactionalProducer();
        transactionalProducer.startProducer();
    }
}
