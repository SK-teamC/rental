package rental;

import com.fasterxml.jackson.datatype.jsr310.deser.YearMonthDeserializer;
import rental.config.kafka.KafkaProcessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Service
public class PolicyHandler{

    @Autowired
    CheckRepository checkRepository;

    @StreamListener(KafkaProcessor.INPUT)
    public void onStringEventListener(@Payload String eventString){

    }

    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverOrdered_ScheduleFix(@Payload Ordered ordered) throws ParseException {

        if(ordered.isMe()){

            Check check = new Check();
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            Date   dateCheckDate       = format.parse ( ordered.getStartYmd());

            check.setCheckDate(format.format(dateCheckDate));
            check.setOrderId(ordered.getId());

            checkRepository.save(check);
            System.out.println("##### listener ScheduleFix : " + ordered.toJson());
        }
    }

}
