package com.carincho.course.springcloud.kafka.api.services;

import com.carincho.course.springcloud.kafka.api.models.Reply;
import com.carincho.course.springcloud.kafka.api.models.dto.ProductDto;

import java.time.Duration;

public interface ProductCommandService {

    //Ahora ya no solo envia si no que ahora espera una respuesta
    Reply<?> sendCreateAndAwait(ProductDto productDto, Duration timeout);
    Reply<?> sendReadAndAwait(Long id, Duration timeout);
    Reply<?> sendReadAllAndAwait(Duration timeout);
    Reply<?>sendUpdateAndAwait(ProductDto productDto, Long id, Duration timeout);
    Reply<?> sendDeleteAndAwait(Long id, Duration timeout);
}
