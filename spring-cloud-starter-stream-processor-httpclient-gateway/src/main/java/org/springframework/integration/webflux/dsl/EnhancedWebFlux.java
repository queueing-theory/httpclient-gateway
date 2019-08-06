package org.springframework.integration.webflux.dsl;

import org.springframework.expression.Expression;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.messaging.Message;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.function.Function;

/**
 * The WebFlux components Factory.
 *
 * @author Artem Bilan
 * @author Shiliang Li
 *
 * @since 5.0
 */
public final class EnhancedWebFlux {
    /**
     * Create an {@link WebFluxMessageHandlerSpec} builder for request-reply gateway
     * based on provided {@code Function} to evaluate target {@code uri} against request message
     * and {@link WebClient} for HTTP exchanges.
     * @param uriFunction the {@code Function} to evaluate {@code uri} at runtime.
     * @param webClient {@link WebClient} to use.
     * @param <P> the expected payload type.
     * @return the WebFluxMessageHandlerSpec instance
     */
    public static <P> EnhancedWebFluxMessageHandlerSpec outboundGateway(Function<Message<P>, ?> uriFunction,
                                                                WebClient webClient) {
        return outboundGateway(new FunctionExpression<>(uriFunction), webClient);
    }

    /**
     * Create an {@link WebFluxMessageHandlerSpec} builder for request-reply gateway
     * based on provided SpEL {@link Expression} to evaluate target {@code uri}
     * against request message and {@link WebClient} for HTTP exchanges.
     * @param uriExpression the SpEL {@link Expression} to evaluate {@code uri} at runtime.
     * @param webClient {@link WebClient} to use.
     * @return the WebFluxMessageHandlerSpec instance
     */
    public static EnhancedWebFluxMessageHandlerSpec outboundGateway(Expression uriExpression,
                                                            WebClient webClient) {
        return new EnhancedWebFluxMessageHandlerSpec(uriExpression, webClient);
    }

    private EnhancedWebFlux() {
        super();
    }
}
