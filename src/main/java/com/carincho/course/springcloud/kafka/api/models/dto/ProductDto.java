package com.carincho.course.springcloud.kafka.api.models.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
// los atributos son final no se pueden modificar, Crea metodos getter y setter sin los prefijos get y set

public record ProductDto(@NotBlank String name, @NotNull @Min(10) Double price) {
}
