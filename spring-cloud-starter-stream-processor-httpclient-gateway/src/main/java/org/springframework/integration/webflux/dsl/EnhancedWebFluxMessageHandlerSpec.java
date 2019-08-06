package org.springframework.integration.webflux.dsl;

import org.springframework.expression.Expression;
import org.springframework.http.client.reactive.ClientHttpResponse;
import org.springframework.integration.http.dsl.BaseHttpMessageHandlerSpec;
import org.springframework.integration.webflux.outbound.EnhancedWebFluxRequestExecutingMessageHandler;
import org.springframework.integration.webflux.outbound.WebFluxRequestExecutingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.web.reactive.function.BodyExtractor;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.function.Function;

public class EnhancedWebFluxMessageHandlerSpec
        extends BaseHttpMessageHandlerSpec<EnhancedWebFluxMessageHandlerSpec, EnhancedWebFluxRequestExecutingMessageHandler> {

    private final WebClient webClient;

    public EnhancedWebFluxMessageHandlerSpec(Expression uriExpression, WebClient webClient) {
        super(new EnhancedWebFluxRequestExecutingMessageHandler(uriExpression, webClient));
        this.webClient = webClient;
    }

    /**
     * The boolean flag to identify if the reply payload should be as a
     * {@link reactor.core.publisher.Flux} from the response body
     * or as resolved value from the {@link reactor.core.publisher.Mono}
     * of the response body.
     * Defaults to {@code false} - simple value is pushed downstream.
     * Makes sense when {@code expectedResponseType} is configured.
     * @param replyPayloadToFlux represent reply payload as a
     * {@link reactor.core.publisher.Flux} or as a value from the
     * {@link reactor.core.publisher.Mono}.
     * @return the spec
     * @since 5.0.1
     * @see EnhancedWebFluxRequestExecutingMessageHandler#setReplyPayloadToFlux(boolean)
     */
    public EnhancedWebFluxMessageHandlerSpec replyPayloadToFlux(boolean replyPayloadToFlux) {
        this.target.setReplyPayloadToFlux(replyPayloadToFlux);
        return this;
    }

    /**
     * Specify a function that returns a {@link BodyExtractor} determined by Message<?>. Used as an alternative to the {@code expectedResponseType}
     * to allow to get low-level access to the received {@link ClientHttpResponse}.
     * @param bodyExtractorFunction a function returning a {@link BodyExtractor} to use.
     * @return the spec
     * @since 5.0.1
     * @see EnhancedWebFluxRequestExecutingMessageHandler#setBodyExtractorFunction(Function<Message<?>, BodyExtractor<?, ClientHttpResponse>>)
     */
    public EnhancedWebFluxMessageHandlerSpec bodyExtractorFunction(Function<Message<?>, BodyExtractor<?, ClientHttpResponse>> bodyExtractorFunction) {
        this.target.setBodyExtractorFunction(bodyExtractorFunction);
        return this;
    }

    @Override
    protected boolean isClientSet() { return this.webClient != null; }

    protected EnhancedWebFluxMessageHandlerSpec expectReply(boolean expectReply) {return  super.expectReply(expectReply);}
}
