package rental;

import rental.config.kafka.KafkaProcessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class PolicyHandler{
    @StreamListener(KafkaProcessor.INPUT)
    public void onStringEventListener(@Payload String eventString){

    }

    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverOrderCanceled_ProductChange(@Payload OrderCanceled orderCanceled){

        if(orderCanceled.isMe()){
            System.out.println("##### listener ProductChange : " + orderCanceled.toJson());
        }
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverOrdered_ProductChange(@Payload Ordered ordered){

        if(ordered.isMe()){
            System.out.println("##### listener ProductChange : " + ordered.toJson());
        }
    }

}
