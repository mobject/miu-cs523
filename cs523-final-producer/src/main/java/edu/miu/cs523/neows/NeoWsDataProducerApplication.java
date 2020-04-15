package edu.miu.cs523.neows;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;

@SpringBootApplication
public class NeoWsDataProducerApplication {

    public static void main(String[] args) throws InterruptedException {
        ConfigurableApplicationContext context = SpringApplication.run(NeoWsDataProducerApplication.class, args);
        MessageProducer producer = context.getBean(MessageProducer.class);
        NeoWSDataService neoWSDataService = context.getBean(NeoWSDataService.class);
        LocalDate localDate = LocalDate.of(2020, 01, 01);
        while (true) {

            NeoWSData neoWSData = neoWSDataService.fetchData(localDate);
            producer.sendMessage(neoWSData);
            Thread.sleep(1000);
            localDate = localDate.plusDays(1);
        }

//        context.close();
    }

    @Bean
    public MessageProducer messageProducer() {
        return new MessageProducer();
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }


    public static class MessageProducer {

        @Autowired
        private KafkaTemplate<Object, NeoWSData> kafkaTemplate;

        @Value(value = "${message.topic.name}")
        private String topicName;

        public void sendMessage(NeoWSData neoWSData) {

            ListenableFuture<SendResult<Object, NeoWSData>> future = kafkaTemplate.send(topicName, neoWSData);

            future.addCallback(new ListenableFutureCallback<SendResult<Object, NeoWSData>>() {

                @Override
                public void onSuccess(SendResult<Object, NeoWSData> result) {
                    System.out.println("Sent message=[] with offset=[" + result.getRecordMetadata() + "]");
                }

                @Override
                public void onFailure(Throwable ex) {
                    System.out.println("Unable to send message=[] due to : " + ex.getMessage());
                }
            });
        }

    }
}
