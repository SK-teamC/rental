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

    @Autowired
    DeliveryRepository deliveryRepository;

    @StreamListener(KafkaProcessor.INPUT)
    public void onStringEventListener(@Payload String eventString){

    }

    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverOrdered_Delivery(@Payload Ordered ordered){

        if(ordered.isMe()){
            Delivery delivery = new Delivery();
            delivery.setOrderId(ordered.getId());
            delivery.setStatus("Delivery Start!");
            deliveryRepository.save(delivery);
            System.out.println("##### listener Delivery : " + ordered.toJson());
        }
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverOrderCanceled_DeliveryCancel(@Payload OrderCanceled orderCanceled){

        if(orderCanceled.isMe()){
            Delivery cancelDelivery = deliveryRepository.findByOrderId(orderCanceled.getId());
            if (cancelDelivery != null)
                cancelDelivery.setStatus("Delivery Cancel!");
            deliveryRepository.save(cancelDelivery);
            System.out.println("##### listener DeliveryCancel : " + orderCanceled.toJson());
        }
    }

}
