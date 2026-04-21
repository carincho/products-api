package com.carincho.course.springcloud.kafka.api.services;

import com.carincho.course.springcloud.kafka.api.models.Command;
import com.carincho.course.springcloud.kafka.api.models.dto.ProductDto;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

@Service
public class ProductCommandServiceImpl implements  ProductCommandService {

    private final StreamBridge bridge;

    public ProductCommandServiceImpl(StreamBridge bridge){
        this.bridge = bridge;
    }

    @Override
    public void sendCreate(ProductDto productDto) {

        Command<ProductDto> cmd = new Command<>("CREATE", null, productDto);
        //Dar un nombre logico un canal de counicacion un puente un nombre
        //out por que es de salida
        boolean sent = this.bridge.send("commands-out-0", cmd);

        if(!sent) {
            throw new IllegalStateException("commands not sent");
        }

    }
}
