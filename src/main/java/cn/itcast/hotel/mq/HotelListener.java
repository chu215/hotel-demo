package cn.itcast.hotel.mq;

import cn.itcast.hotel.service.IHotelService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static cn.itcast.hotel.constants.MqConstants.HOTEL_DELETE_QUEUE;
import static cn.itcast.hotel.constants.MqConstants.HOTEL_INSERT_QUEUE;

@Component
public class HotelListener {

    @Autowired
    private IHotelService hotelService;

    @RabbitListener(queues = HOTEL_INSERT_QUEUE)
    public void listenHotelInsertOrUpdate(Long id) {
        hotelService.insertById(id);
    }

    @RabbitListener(queues = HOTEL_DELETE_QUEUE)
    public void listenHotelDelete(Long id) {
        hotelService.deleteById(id);
    }
}
