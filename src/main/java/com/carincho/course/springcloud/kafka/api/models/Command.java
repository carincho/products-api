package com.carincho.course.springcloud.kafka.api.models;


//Es un objeto de datos de kafkac
// body queda generico

//El type es el que dice que tipo de instruccion se va a generar en kafka este es va a create
public record Command<T>(String type, Long id, T body) {
}
