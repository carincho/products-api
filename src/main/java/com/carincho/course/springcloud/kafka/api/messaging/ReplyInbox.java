package com.carincho.course.springcloud.kafka.api.messaging;

import com.carincho.course.springcloud.kafka.api.models.Reply;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/*Es el contenedor para almacenar la respuesta por que es asincrona*/
@Component
public class ReplyInbox {

    //ConcurrentHashMap es una clase que esta diseñada para almacenar valores como un diccionario, pemite almacenar
    // valores de forma segura en un entorno donde hay muchos hilos que estan funcionando que acceden a los datos de forma
//    simultanea por eso se usa esto
    //CompletableFuture representa el resultado de una tarea que se esta ejecutando de forma asincrona y su resultado se va a
    // completar en el futuro
    private final ConcurrentHashMap<String, CompletableFuture<Reply<?>>> pending = new ConcurrentHashMap<>();

    //Metodo que se va a completar en el futuro CompletableFuture simplemnte es una tajera que contiene informacion
//    que todavia no se ha completado


    public CompletableFuture<Reply<?>> register(String correlationId) {
        CompletableFuture<Reply<?>> future = new CompletableFuture<>();
        //Guarda la respuesta pero despues se va a completar sola mente  se guarda queda sin valor con respuesta
//        pendiente
        pending.put(correlationId, future);
        return future;

    }

    //Aqui se va a completar cuando el cliente ya obtenga la respuesta de kafka y del otro microservicio command
    public void complete(String correlationId, Reply<?> reply) {
        //Se elimina la respuesta para completarlo se tiene que remover del repositorio
//        Devuelve el valor asociado a la llave el valor futuro y ese es el que se completa

        if(correlationId == null) {
            throw new IllegalArgumentException("correlationId must not be null");
        };

        CompletableFuture<Reply<?>> future = pending.remove(correlationId);

        if (future != null) {
            future.complete(reply);
        }
    }
}
