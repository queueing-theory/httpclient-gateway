package org.springframework.integration.webflux.outbound;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.http.*;
import org.springframework.http.client.reactive.ClientHttpResponse;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.http.outbound.AbstractHttpRequestExecutingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.web.reactive.function.BodyExtractor;
import org.springframework.web.reactive.function.BodyExtractors;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A {@link org.springframework.messaging.MessageHandler} implementation that executes
 * HTTP requests by delegating to a Reactive {@link WebClient} instance.
 *
 * @author Shiliang Li
 * @author Artem Bilan
 * @author Gary Russell
 * @author Haruhiko Nishi
 * @see org.springframework.integration.http.outbound.HttpRequestExecutingMessageHandler
 * @since 5.0
 */
public class EnhancedWebFluxRequestExecutingMessageHandler extends AbstractHttpRequestExecutingMessageHandler {

    private final WebClient webClient;

    private boolean replyPayloadToFlux;

    private Function<Message<?>, BodyExtractor<?, ClientHttpResponse>> bodyExtractorFunction;

    /**
     * Create a handler that will send requests to the provided URI.
     *
     * @param uri The URI.
     */
    public EnhancedWebFluxRequestExecutingMessageHandler(URI uri) {
        this(new ValueExpression<>(uri));
    }

    /**
     * Create a handler that will send requests to the provided URI.
     *
     * @param uri The URI.
     */
    public EnhancedWebFluxRequestExecutingMessageHandler(String uri) {
        this(uri, null);
    }

    /**
     * Create a handler that will send requests to the provided URI Expression.
     *
     * @param uriExpression The URI expression.
     */
    public EnhancedWebFluxRequestExecutingMessageHandler(Expression uriExpression) {
        this(uriExpression, null);
    }

    /**
     * Create a handler that will send requests to the provided URI using a provided WebClient.
     *
     * @param uri       The URI.
     * @param webClient The WebClient to use.
     */
    public EnhancedWebFluxRequestExecutingMessageHandler(String uri, WebClient webClient) {
        this(new LiteralExpression(uri), webClient);
        /*
         *  We'd prefer to do this assertion first, but the compiler doesn't allow it. However,
         *  it's safe because the literal expression simply wraps the String variable, even
         *  when null.
         */
        Assert.hasText(uri, "URI is required");
    }

    public EnhancedWebFluxRequestExecutingMessageHandler(Expression uriExpression, WebClient webClient) {
        super(uriExpression);
        this.webClient = (webClient == null ? WebClient.create() : webClient);
        this.setAsync(true);
    }

    /**
     * The boolean flag to identify if the reply payload should be as a {@link Flux} from the response body
     * or as resolved value from the {@link Mono} of the response body.
     * Defaults to {@code false} - simple value is pushed downstream.
     * Makes sense when {@code expectedResponseType} is configured.
     *
     * @param replyPayloadToFlux represent reply payload as a {@link Flux} or as a value from the {@link Mono}.
     * @see #setExpectedResponseType(Class)
     * @see #setExpectedResponseTypeExpression(Expression)
     * @since 5.0.1
     */
    public void setReplyPayloadToFlux(boolean replyPayloadToFlux) {
        this.replyPayloadToFlux = replyPayloadToFlux;
    }

    /**
     * Specify a {@link BodyExtractor} as an alternative to the {@code expectedResponseType}
     * to allow to get low-level access to the received {@link ClientHttpResponse}.
     *
     * @param bodyExtractorFunction the {@link BodyExtractor} to use.
     * @see #setExpectedResponseType(Class)
     * @see #setExpectedResponseTypeExpression(Expression)
     * @since 5.0.1
     */
    public void setBodyExtractorFunction(Function<Message<?>, BodyExtractor<?, ClientHttpResponse>> bodyExtractorFunction) {
        this.bodyExtractorFunction = bodyExtractorFunction;
    }

    @Override
    public String getComponentType() {
        return (isExpectReply() ? "webflux:outbound-gateway" : "webflux:outbound-channel-adapter");
    }

    @Override
    protected Object exchange(Supplier<URI> uriSupplier, HttpMethod httpMethod, HttpEntity<?> httpRequest,
                              Object expectedResponseType, Message<?> requestMessage) {

        WebClient.RequestBodySpec requestSpec =
                this.webClient.method(httpMethod)
                        .uri(b -> uriSupplier.get())
                        .headers(headers -> headers.putAll(httpRequest.getHeaders()));

        if (httpRequest.hasBody()) {
            requestSpec.body(BodyInserters.fromObject(httpRequest.getBody())); // NOSONAR protected with hasBody()
        }

        Mono<ClientResponse> responseMono =
                requestSpec.exchange()
                        .flatMap(response -> {
                            HttpStatus httpStatus = response.statusCode();
                            if (httpStatus.isError()) {
                                return response.body(BodyExtractors.toDataBuffers())
                                        .reduce(DataBuffer::write)
                                        .map(dataBuffer -> {
                                            byte[] bytes = new byte[dataBuffer.readableByteCount()];
                                            dataBuffer.read(bytes);
                                            DataBufferUtils.release(dataBuffer);
                                            return bytes;
                                        })
                                        .defaultIfEmpty(new byte[0])
                                        .map(bodyBytes -> {
                                                    throw new WebClientResponseException(
                                                            "ClientResponse has erroneous status code: "
                                                                    + httpStatus.value() + " "
                                                                    + httpStatus.getReasonPhrase(),
                                                            httpStatus.value(),
                                                            httpStatus.getReasonPhrase(),
                                                            response.headers().asHttpHeaders(),
                                                            bodyBytes,
                                                            response.headers().contentType()
                                                                    .map(MimeType::getCharset)
                                                                    .orElse(StandardCharsets.ISO_8859_1));
                                                }
                                        );
                            } else {
                                return Mono.just(response);
                            }
                        });

        if (isExpectReply()) {
            return responseMono
                    .flatMap(response -> {
                                ResponseEntity.BodyBuilder httpEntityBuilder =
                                        ResponseEntity.status(response.statusCode())
                                                .headers(response.headers().asHttpHeaders());

                                Mono<?> bodyMono;

                                if (expectedResponseType != null) {
                                    if (this.replyPayloadToFlux) {
                                        BodyExtractor<? extends Flux<?>, ReactiveHttpInputMessage> extractor;
                                        if (expectedResponseType instanceof ParameterizedTypeReference<?>) {
                                            extractor = BodyExtractors.toFlux(
                                                    (ParameterizedTypeReference<?>) expectedResponseType);
                                        } else {
                                            extractor = BodyExtractors.toFlux((Class<?>) expectedResponseType);
                                        }
                                        Flux<?> flux = response.body(extractor);
                                        bodyMono = Mono.just(flux);
                                    } else {
                                        BodyExtractor<? extends Mono<?>, ReactiveHttpInputMessage> extractor;
                                        if (expectedResponseType instanceof ParameterizedTypeReference<?>) {
                                            extractor = BodyExtractors.toMono(
                                                    (ParameterizedTypeReference<?>) expectedResponseType);
                                        } else {
                                            extractor = BodyExtractors.toMono((Class<?>) expectedResponseType);
                                        }
                                        bodyMono = response.body(extractor);
                                    }
                                } else if (this.bodyExtractorFunction != null) {
                                    Object body = response.body(this.bodyExtractorFunction.apply(requestMessage));
                                    if (body instanceof Mono) {
                                        bodyMono = (Mono<?>) body;
                                    } else {
                                        bodyMono = Mono.just(body);
                                    }
                                } else {
                                    bodyMono = Mono.empty();
                                }

                                return bodyMono
                                        .map(httpEntityBuilder::body)
                                        .defaultIfEmpty(httpEntityBuilder.build());
                            }
                    )
                    .map(this::getReply);
        } else {
            responseMono.subscribe(v -> {
            }, ex -> sendErrorMessage(requestMessage, ex));

            return null;
        }
    }
}
