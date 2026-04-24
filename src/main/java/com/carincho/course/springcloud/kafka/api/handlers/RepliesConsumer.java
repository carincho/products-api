package com.carincho.course.springcloud.kafka.api.handlers;

import com.carincho.course.springcloud.kafka.api.messaging.ReplyInbox;
import com.carincho.course.springcloud.kafka.api.models.Reply;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import java.util.function.Consumer;

@Configuration
public class RepliesConsumer {

    private final ReplyInbox replyInbox;

    public RepliesConsumer(ReplyInbox replyInbox) {
        this.replyInbox = replyInbox;
    }

    @Bean
    public Consumer<Message<Reply<?>>> handleReplies() {

        return message -> {
          String correlationId = message.getHeaders().get("correlationId", String.class);
          replyInbox.complete(correlationId, message.getPayload());

        };
    }

}
