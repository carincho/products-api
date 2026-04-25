package com.carincho.course.springcloud.kafka.api.controllers;

import com.carincho.course.springcloud.kafka.api.models.Reply;
import com.carincho.course.springcloud.kafka.api.models.ReplyStatus;
import com.carincho.course.springcloud.kafka.api.models.dto.ProductDto;
import com.carincho.course.springcloud.kafka.api.services.ProductCommandService;
import jakarta.validation.Valid;
import org.jspecify.annotations.NonNull;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
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

        return getResponseEntity(service.sendCreateAndAwait(productDto, Duration.ofSeconds(5)));

    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getProductById(@PathVariable Long id) {

        return getResponseEntity(service.sendReadAndAwait(id, Duration.ofSeconds(5)));

    }

    @GetMapping
    public ResponseEntity<?> getAllProducts() {

        return getResponseEntity(service.sendReadAllAndAwait(Duration.ofSeconds(5)));
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> updateProduct(@PathVariable Long id, @Valid @RequestBody ProductDto productDto) {

        return getResponseEntity(service.sendUpdateAndAwait(productDto, id, Duration.ofSeconds(5)));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteProduct(@PathVariable Long id) {

        return getResponseEntity(service.sendDeleteAndAwait(id, Duration.ofSeconds(5)));
    }

    private static @NonNull ResponseEntity<?> getResponseEntity(Reply<?> reply) {
        if(reply.status().isSuccess()) {
            return ResponseEntity.ok().body(reply.body());
        }

        return ResponseEntity.badRequest().body(Map.of("error", reply.message()));
    }

}
