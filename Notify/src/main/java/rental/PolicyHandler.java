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
    public void wheneverOrdered_Notify(@Payload Ordered ordered){

        if(ordered.isMe()){
            System.out.println("##### listener Notify : " + ordered.toJson());
        }
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverChecked_Notify(@Payload Checked checked){

        if(checked.isMe()){
            System.out.println("##### listener Notify : " + checked.toJson());
        }
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverScheduleFixed_Notify(@Payload ScheduleFixed scheduleFixed){

        if(scheduleFixed.isMe()){
            System.out.println("##### listener Notify : " + scheduleFixed.toJson());
        }
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverPaid_Notify(@Payload Paid paid){

        if(paid.isMe()){
            System.out.println("##### listener Notify : " + paid.toJson());
        }
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverPayCanceled_Notify(@Payload PayCanceled payCanceled){

        if(payCanceled.isMe()){
            System.out.println("##### listener Notify : " + payCanceled.toJson());
        }
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverDelivered_Notify(@Payload Delivered delivered){

        if(delivered.isMe()){
            System.out.println("##### listener Notify : " + delivered.toJson());
        }
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverDeliveryCanceled_Notify(@Payload DeliveryCanceled deliveryCanceled){

        if(deliveryCanceled.isMe()){
            System.out.println("##### listener Notify : " + deliveryCanceled.toJson());
        }
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverOrdered_Notify(@Payload Ordered ordered){

        if(ordered.isMe()){
            System.out.println("##### listener Notify : " + ordered.toJson());
        }
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverOrderCanceled_Notify(@Payload OrderCanceled orderCanceled){

        if(orderCanceled.isMe()){
            System.out.println("##### listener Notify : " + orderCanceled.toJson());
        }
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverDeliveryCanceled_Notify(@Payload DeliveryCanceled deliveryCanceled){

        if(deliveryCanceled.isMe()){
            System.out.println("##### listener Notify : " + deliveryCanceled.toJson());
        }
    }

}
