package com.carincho.course.springcloud.kafka.api.controllers;

import com.carincho.course.springcloud.kafka.api.models.Reply;
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
        Reply<?> reply = service.sendCreateAndAwait(productDto, Duration.ofSeconds(5));

        return getResponseEntity(reply);

    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getProductById(@PathVariable Long id) {

        Reply<?> reply = service.sendReadAndAwait(id, Duration.ofSeconds(5));
        return getResponseEntity(reply);

    }

    @GetMapping
    public ResponseEntity<?> getAllProducts() {
        Reply<?> reply = service.sendReadAllAndAwait(Duration.ofSeconds(5));

        return getResponseEntity(reply);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> updateProduct(@PathVariable Long id, @Valid @RequestBody ProductDto productDto) {

        Reply<?> reply = service.sendUpdateAndAwait(productDto, id, Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteProduct(@PathVariable Long id) {

        Reply<?> reply = service.sendDeleteAndAwait(id, Duration.ofSeconds(5));
        return getResponseEntity(reply);
    }

    private static @NonNull ResponseEntity<?> getResponseEntity(Reply<?> reply) {
        if("SUCCESS".equalsIgnoreCase(reply.status())) {
            return ResponseEntity.ok().body(reply.body());
        }

        return ResponseEntity.badRequest().body(Map.of("error", reply.message()));
    }

}
