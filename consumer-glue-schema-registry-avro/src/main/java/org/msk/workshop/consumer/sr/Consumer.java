package org.msk.workshop.consumer.sr;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    private final static Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
    private Properties applicationProperties;
    private String bootstrapServers;
    public Consumer(String bootstrapServers){
        this.bootstrapServers = bootstrapServers;
    }
    private void readProperties() throws IOException {
        logger.info("reading app.properties...");
        InputStream iStream = this.getClass().getClassLoader().getResourceAsStream("app.properties");
        this.applicationProperties = new Properties();
        this.applicationProperties.load(iStream);
    }
    public Properties buildConsumerConfig() throws IOException{
        logger.info("building producer configuration...");
        if(this.applicationProperties == null)
            this.readProperties();
        Properties consumerProperties = new Properties();
        //for async client, use the following "software.amazon.awssdk.http.async.service.impl" and "software.amazon.awssdk.http.nio.netty.NettySdkAsyncHttpService"
        System.setProperty("software.amazon.awssdk.http.service.impl", "software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService");
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG,this.applicationProperties.getProperty("GROUP_ID"));
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.applicationProperties.getProperty("AUTO_OFFSET_RESET_CONFIG"));
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaDeserializer.class.getName());
        consumerProperties.put(AWSSchemaRegistryConstants.AWS_REGION, this.applicationProperties.getProperty("AWS_REGION"));
        consumerProperties.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
        return consumerProperties;
    }
    public void startConsumer() throws IOException, InterruptedException {
        Properties consumerConfig = this.buildConsumerConfig();
        String topic = this.applicationProperties.getProperty("TOPIC");
        logger.info("starting consumer...");
        try (final KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(consumerConfig)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (true) {
                final ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
                for (final ConsumerRecord<String, GenericRecord> record : records) {
                    final GenericRecord value = record.value();
                    logger.info("Received message: value = " + value);
                }
            }
        } catch (final SerializationException e) {
            e.printStackTrace();
        }
    }
    public static void main(final String[] args) throws IOException, InterruptedException {
        //it expects arg[0] as a broker address
        Consumer consumer = new Consumer(args[0]);
        consumer.startConsumer();
    }
}