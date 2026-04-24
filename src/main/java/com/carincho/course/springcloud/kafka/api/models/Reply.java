package com.carincho.course.springcloud.kafka.api.models;

public record Reply<T>(String status, String message, T body) {
}
