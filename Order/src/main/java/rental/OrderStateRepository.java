package rental;

import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface OrderStateRepository extends CrudRepository<OrderState, Long> {

    List<OrderState> findByOrderId(Long orderId);
    List<OrderState> findByOrderId(Long orderId);
    List<OrderState> findByOrderId(Long orderId);
    List<OrderState> findByOrderId(Long orderId);
    List<OrderState> findByOrderId(Long orderId);

        void deleteBy( );
}