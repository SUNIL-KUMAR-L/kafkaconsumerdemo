package com.example.kafkaconsumerdemo;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

@Configuration
public class AppConfig {

    public static final String SECURE_TOPIC_CONNECTION = "secureTopicConnection";


    @Bean
    KafkaConsumer<String, String> buildKafkaConsumer(Environment environment) {
        Properties kafkaConsumerConfig = new Properties();
//        kafkaConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        kafkaConsumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
//        kafkaConsumerConfig.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1" );
//        kafkaConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
//        kafkaConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//        kafkaConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        kafkaConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        //kafkaConsumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, environment.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        kafkaConsumerConfig.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, environment.getProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
        String kafkaConsumerGroupId = environment.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        if(kafkaConsumerGroupId == null || kafkaConsumerGroupId.trim().length() <= 0) {
            kafkaConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        } else {
            kafkaConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerGroupId);
        }
        kafkaConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, environment.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        kafkaConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, environment.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));

        setupKafkaSSLproperties(environment, kafkaConsumerConfig);

        System.out.println("kafkaConsumerConfig="+ kafkaConsumerConfig);

        KafkaConsumer kafkaConsumer = new KafkaConsumer(kafkaConsumerConfig, new StringDeserializer(), new StringDeserializer());


        return kafkaConsumer;

    }

    @Bean
    ApplicationRunner runKafkaConsumer(KafkaConsumer<String, String > kafkaConsumer, Environment environment) {

        return  new ApplicationRunner() {
            @Override
            public void run(ApplicationArguments args) throws Exception {

                //kafkaConsumer.subscribe(Arrays.asList("my-demo-topic"));
                kafkaConsumer.subscribe(Arrays.asList(environment.getProperty("topic-name")));

                System.out.println("topic-name="+ environment.getProperty("topic-name"));

                Long pollTime = getPollTime(environment);

                ConsumerRecords<String, String> consumerRecords = null;
                while (true) {
                    consumerRecords = kafkaConsumer.poll(pollTime);
                    if(!consumerRecords.isEmpty()) {
                        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                            System.err.println(consumerRecord);
                            String key = consumerRecord.key();
                            String value = consumerRecord.value();
                            System.err.println("offset="+consumerRecord.offset()+",key=" + consumerRecord.key() +",value=" + consumerRecord.value());
                            // below lines were specific to my need
                            key = key.replaceAll("'","");
                            value = value.replaceAll("'","");
                            System.err.println("offset="+consumerRecord.offset()+",key=" + key +",value=" + value);
                            FileCopyUtils.copy("offset="+consumerRecord.offset()+",key=" + key +",value=" + value + "\n", new FileWriter("key-value-offset.txt", true));
                            FileCopyUtils.copy(value, new FileWriter(key+".json"));
                        }
                    }
                    kafkaConsumer.commitSync();
                }
            }
        };
    }

    private Long getPollTime(Environment environment) {
        Long pollms = 100l;
        try {
            Long kafkaConsumerPoll = environment.getProperty("kafkaConsumer.poll", Long.class);
            System.out.println("kafkaConsumer.poll="+ kafkaConsumerPoll);
            if(kafkaConsumerPoll != null) {
                pollms =  kafkaConsumerPoll;
            }
        } catch (Exception  exp){
            exp.printStackTrace();
        }
        return pollms;
    }

    private void setupKafkaSSLproperties(Environment environment, Properties properties) {
        if ("true".equals(environment.getProperty(SECURE_TOPIC_CONNECTION, "false"))) {

            String securityProtocol = environment.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");

            String sslTruststoreLocation = environment.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
            Objects.requireNonNull(sslTruststoreLocation, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);

            String sslTruststorePassword = environment.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
            Objects.requireNonNull(sslTruststorePassword, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);

            String sslKeystoreLocation = environment.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
            Objects.requireNonNull(sslKeystoreLocation, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);

            String sslKeystorePassword = environment.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
            Objects.requireNonNull(sslKeystorePassword, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);

            String sslKeyPassword = environment.getProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
            Objects.requireNonNull(sslKeyPassword, SslConfigs.SSL_KEY_PASSWORD_CONFIG);

            properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            properties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
            properties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
            properties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreLocation);
            properties.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
            properties.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
        }
    }


}
