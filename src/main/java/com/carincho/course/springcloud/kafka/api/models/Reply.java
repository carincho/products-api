package com.carincho.course.springcloud.kafka.api.models;

public record Reply<T>(ReplyStatus status, String message, T body) {
}
