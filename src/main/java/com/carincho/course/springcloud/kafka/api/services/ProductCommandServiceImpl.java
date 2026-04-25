package com.carincho.course.springcloud.kafka.api.services;

import com.carincho.course.springcloud.kafka.api.messaging.ReplyInbox;
import com.carincho.course.springcloud.kafka.api.models.Command;
import com.carincho.course.springcloud.kafka.api.models.CommandType;
import com.carincho.course.springcloud.kafka.api.models.Reply;
import com.carincho.course.springcloud.kafka.api.models.dto.ProductDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class ProductCommandServiceImpl implements  ProductCommandService {

    private final StreamBridge bridge;
    private final ReplyInbox replyInbox;
    private static final Logger logger = LoggerFactory.getLogger(ProductCommandServiceImpl.class);

    public ProductCommandServiceImpl(StreamBridge bridge,  ReplyInbox replyInbox) {

        this.bridge = bridge;
        this.replyInbox = replyInbox;

    }

    @Override
    public  Reply<?> sendCreateAndAwait(ProductDto productDto, Duration timeout) {

        //Dar un nombre logico un canal de counicacion un puente un nombre
        //out por que es de salida

        return sendAndAwait(new Command<>(CommandType.CREATE, null, productDto), timeout);

    }

    @Override
    public Reply<?> sendReadAndAwait(Long id, Duration timeout) {
        return sendAndAwait(new Command<>(CommandType.READ, id, null), timeout);
    }

    @Override
    public Reply<?> sendReadAllAndAwait(Duration timeout) {

        return sendAndAwait(new Command<>(CommandType.READ_ALL, null, null), timeout);
    }

    @Override
    public Reply<?> sendUpdateAndAwait(ProductDto productDto, Long id, Duration timeout) {
        return sendAndAwait(new Command<>(CommandType.UPDATE, id, productDto), timeout);
    }

    @Override
    public Reply<?> sendDeleteAndAwait(Long id, Duration timeout) {


        return sendAndAwait(new Command<>(CommandType.DELETE, id, null), timeout);
    }

    private Reply<?> sendAndAwait(Command<?> cmd, Duration timeout) {
        //Dar un nombre logico un canal de counicacion un puente un nombre
        //out por que es de salida

        String correlationId = UUID.randomUUID().toString();

        logger.info("Api productos client Creating product to correlationId {}", correlationId);
        CompletableFuture<Reply<?>> future = replyInbox.register(correlationId);

        //Message es una forma de mandar mensajes con configuraciones con metadata al topic
        var message = MessageBuilder.withPayload(cmd)
                .setHeader("correlationId", correlationId).build();

        //Aqui se enviaba el cmd a kafka
//;        boolean sent = this.bridge.send("commands-out-0", cmd);

        //Ahora se enviara el message por que ahora debe enviarse el correlationID como identificador
        boolean sent = this.bridge.send("commands-out-0", message);


        if(!sent) {
            throw new IllegalStateException("commands not sent");
        }

        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Timeout esperando respuesta de products-command desde kafka", e);
        }
    }
}
