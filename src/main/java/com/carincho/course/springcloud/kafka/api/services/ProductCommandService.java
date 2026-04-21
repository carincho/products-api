package com.carincho.course.springcloud.kafka.api.services;

import com.carincho.course.springcloud.kafka.api.models.dto.ProductDto;

public interface ProductCommandService {

    void sendCreate(ProductDto productDto);
}
