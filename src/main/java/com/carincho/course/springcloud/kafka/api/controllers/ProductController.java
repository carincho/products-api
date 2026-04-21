package com.carincho.course.springcloud.kafka.api.controllers;

import com.carincho.course.springcloud.kafka.api.models.dto.ProductDto;
import com.carincho.course.springcloud.kafka.api.services.ProductCommandService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/products")
public class ProductController {

    private final ProductCommandService service;

    public ProductController(ProductCommandService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<?> createProduct(@Valid @RequestBody ProductDto productDto) {
        service.sendCreate(productDto);

        return ResponseEntity.ok().body(Map.of("message", "Success sent"));
    }


}
