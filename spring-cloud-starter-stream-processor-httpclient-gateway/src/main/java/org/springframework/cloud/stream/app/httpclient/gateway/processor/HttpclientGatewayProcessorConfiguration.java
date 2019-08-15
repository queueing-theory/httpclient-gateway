/*
 * Copyright 2015-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.httpclient.gateway.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.app.httpclient.gateway.processor.HttpclientGatewayProcessorConfiguration.HttpclientGatewayProcessor;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ClientHttpResponse;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.handler.advice.RateLimiterRequestHandlerAdvice;
import org.springframework.integration.http.support.DefaultHttpHeaderMapper;
import org.springframework.integration.webflux.dsl.WebFlux;
import org.springframework.integration.webflux.support.ClientHttpResponseBodyExtractor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MimeType;
import org.springframework.util.MultiValueMap;
import org.springframework.util.PathMatcher;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.netty.http.client.HttpClient;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * A processor app that makes requests to an HTTP resource and emits the response body as a message payload. This
 * processor can be combined, e.g., with a time source module to periodically poll results from a HTTP resource.
 *
 * @author Waldemar Hummer
 * @author Mark Fisher
 * @author Gary Russell
 * @author Christian Tzolov
 * @author David Turanski
 * @author Artem Bilan
 * @author Haruhiko Nishi
 */
@EnableBinding(HttpclientGatewayProcessor.class)
@EnableConfigurationProperties(HttpclientGatewayProcessorProperties.class)
@ComponentScan
public class HttpclientGatewayProcessorConfiguration {
    private static final String HTTP_REQUEST_URL_HEADER = "http_requestUrl";
    private static final String HTTP_REQUEST_METHOD_HEADER = "http_requestMethod";
    private static final String HTTP_STATUS_CODE_HEADER = "http_statusCode";
    private static final String CONTINUATION_ID_HEADER = "continuation_id";
    private static final String ORIGINAL_CONTENT_TYPE = "original_content_type";
    private static final String ANALYTICS_FLOW_INPUT = "analyticsFlow.input";
    private static final Duration ONE_SECOND = Duration.ofSeconds(1);
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private final Log logger = LogFactory.getLog(getClass());
    @Autowired
    private HttpclientGatewayProcessorProperties properties;
    @Autowired
    private ResourceLoader resourceLoader;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private PathMatcher pathMatcher;

    @Bean
    IntegrationFlow httpClientFlow(HttpclientGatewayProcessor processor) {

        return IntegrationFlows.from(processor.input())
                .enrichHeaders(f1 -> f1.headerFunction(HTTP_REQUEST_URL_HEADER, m -> {
                    String originalUrl = m.getHeaders().get(HTTP_REQUEST_URL_HEADER, String.class);
                    String url;
                    if (properties.getUrlExpression() != null) {
                        url = properties.getUrlExpression().getValue(m, String.class);
                    } else {
                        url = properties.getUrl();
                    }
                    URI uri = URI.create(url);
                    return UriComponentsBuilder.fromHttpUrl(originalUrl).port(uri.getPort())
                            .scheme(uri.getScheme()).host(uri.getHost()).build().toString();
                }, true))
                .route(Message.class, m -> {
                    String originalContentType = m.getHeaders().get(ORIGINAL_CONTENT_TYPE, String.class);
                    return originalContentType != null && originalContentType.matches("^multipart/form-data.*$");
                }, r -> r.subFlowMapping(true, sf -> sf.gateway(multiPartBody(resourceLoaderSupport())))
                        .subFlowMapping(false, sf -> sf.gateway(body())))
                .headerFilter("host")
                .handle(WebFlux.outboundGateway(m -> m.getHeaders().get(HTTP_REQUEST_URL_HEADER, String.class),
                        WebClient.builder().clientConnector(new ReactorClientHttpConnector(
                                        HttpClient.create().followRedirect(properties.isFollowRedirect())
                                                .wiretap(properties.isWireTap())
                                )
                        ).build()).httpMethodFunction(m -> m.getHeaders().get(HTTP_REQUEST_METHOD_HEADER))
                                .headerMapper(defaultHttpHeaderMapper())
                                .bodyExtractor(new ClientHttpResponseBodyExtractor())
                        , c -> c.advice(rateLimiterRequestHandlerAdvice()))
                .publishSubscribeChannel(executorService(), s -> s.subscribe(responseFlow -> responseFlow.handle(m -> {
                    MessageHeaders messageHeaders = m.getHeaders();
                    ClientHttpResponse clientHttpResponse = (ClientHttpResponse) m.getPayload();
                    HttpHeaders httpHeaders = clientHttpResponse.getHeaders();
                    long contentLength = httpHeaders.getContentLength();
                    Assert.notNull(contentLength, "Content-Length must not be null");
                    DataBufferUtils.join(clientHttpResponse.getBody())
                            .subscribe(buffer -> {
                                URI uri = URI.create(messageHeaders.get(HTTP_REQUEST_URL_HEADER, String.class));

                                if (matchesUrlPatterns(uri.getPath()) ||
                                        contentLength > properties.getContentLengthToExternalize()) {
                                    MediaType mediaType = httpHeaders.getContentType();
                                    String uriPath = null;
                                    try {
                                        uriPath = resourceLoaderSupport()
                                                .externalizeAsResource(uri.getHost() + uri.getPath(), mediaType, buffer);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                    DataBufferUtils.release(buffer);

                                    String requestUrl = messageHeaders.get(HTTP_REQUEST_URL_HEADER, String.class);
                                    ObjectNode objectNode = objectMapper.createObjectNode();
                                    objectNode.put(HTTP_REQUEST_URL_HEADER, requestUrl);
                                    objectNode.put(ORIGINAL_CONTENT_TYPE, messageHeaders.get(MessageHeaders.CONTENT_TYPE).toString());
                                    objectNode.put("uri", uriPath);
                                    Map<String, Object> headersToCopy = messageHeaders.entrySet().stream().filter(headerName -> !headerName.equals(MessageHeaders.CONTENT_TYPE))
                                            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
                                    headersToCopy.put(MessageHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
                                    headersToCopy.put("is_reference", true);
                                    MessageBuilder.withPayload(objectNode).copyHeaders(headersToCopy).build();
                                    Message<? extends JsonNode> message = MessageBuilder.withPayload(objectNode).copyHeaders(headersToCopy).build();
                                    logger.info(message.toString());
                                    processor.output().send(message);
                                } else {
                                    byte[] bytes = new byte[buffer.readableByteCount()];
                                    buffer.read(bytes);
                                    DataBufferUtils.release(buffer);
                                    Message<byte[]> message = MessageBuilder.withPayload(bytes).copyHeaders(messageHeaders).build();
                                    logger.info(message.toString());
                                    processor.output().send(message);
                                }
                            });
                })).subscribe(analyticsFlow -> analyticsFlow.channel(ANALYTICS_FLOW_INPUT))).get();
    }

    @Bean
    public ExecutorService executorService() {
        return Executors.newCachedThreadPool();
    }

    @Bean
    public IntegrationFlow analyticsFlow(HttpclientGatewayProcessor processor) {
        return f -> f.handle(m -> {
            MessageHeaders messageHeaders = m.getHeaders();
            String id = messageHeaders.get(CONTINUATION_ID_HEADER, String.class);
            String httpRequestUrl = messageHeaders.get(HTTP_REQUEST_URL_HEADER, String.class);
            String httpRequestMethod = messageHeaders.get(HTTP_REQUEST_METHOD_HEADER, String.class);
            Object httpStatusCodeValue = messageHeaders.get(HTTP_STATUS_CODE_HEADER);

            HttpStatus httpStatusCode = httpStatusCodeValue instanceof Integer ? HttpStatus.valueOf((Integer) httpStatusCodeValue) : (HttpStatus) httpStatusCodeValue;
            MessageBuilder<?> messageBuilder = MessageBuilder
                    .withPayload((httpStatusCode.is5xxServerError() || httpStatusCode.is4xxClientError()) ? m.getPayload() : EMPTY_BYTE_ARRAY)
                    .setHeader("request_id", Objects.requireNonNull(id))
                    .setHeader("url_requested", Objects.requireNonNull(httpRequestUrl))
                    .setHeader("http_method", Objects.requireNonNull(httpRequestMethod))
                    .setHeader("status_code", Objects.requireNonNull(httpStatusCode).value());
            if (httpStatusCode.is5xxServerError() || httpStatusCode.is4xxClientError()) {
                messageBuilder.setHeader("reason_phrase", httpStatusCode.getReasonPhrase());
            }
            processor.analytics().send(messageBuilder.build());
        });
    }

    private boolean matchesUrlPatterns(String httpRequestUrl) {
        if (properties.getUrlPatternsToExternalize() != null) {
            for (String pattern : properties.getUrlPatternsToExternalize()) {
                if (pathMatcher.match(pattern, httpRequestUrl)) {
                    return true;
                }
            }
        }
        return false;
    }

    private IntegrationFlow body() {
        return f -> f.enrichHeaders(h -> h.headerFunction(MessageHeaders.CONTENT_TYPE, m -> {
            MimeType mimeType = m.getHeaders().get(MessageHeaders.CONTENT_TYPE, MimeType.class);
            return mimeType.toString();
        }, true));
    }

    private IntegrationFlow multiPartBody(ResourceLoaderSupport resourceLoaderSupport) {
        return f -> f.convert(JsonNode.class)
                .<ArrayNode, HttpEntity<?>>transform(p -> {
                    HttpHeaders httpHeaders = new HttpHeaders();
                    httpHeaders.setContentType(MediaType.MULTIPART_FORM_DATA);
                    MultiValueMap<String, Object> multipartRequest = new LinkedMultiValueMap<>();
                    p.forEach(jsonNode -> {
                        HttpHeaders nestedHeaders = new HttpHeaders();
                        String contentType = jsonNode.path("contentType").asText();
                        if (!contentType.isEmpty()) {
                            String originalFileName = jsonNode.path("originalFileName").asText();
                            String formParameterName = jsonNode.path("formParameterName").asText();
                            String uri = jsonNode.path("uri").asText();
                            ContentDisposition contentDisposition = ContentDisposition
                                    .builder("form-data")
                                    .name(formParameterName).filename(originalFileName).build();
                            nestedHeaders.setContentDisposition(contentDisposition);
                            nestedHeaders.setContentType(MediaType.parseMediaType(contentType));
                            Resource resource = resourceLoaderSupport.getResource(uri);
                            HttpEntity<Resource> fileEntity = new HttpEntity<>(resource, nestedHeaders);
                            multipartRequest.add(formParameterName, fileEntity);
                        } else {
                            String name = jsonNode.fieldNames().next();
                            JsonNode value = jsonNode.get(name);
                            if (value.isObject() || value.isArray()) {
                                nestedHeaders.setContentType(MediaType.APPLICATION_JSON);
                                multipartRequest.add(name, new HttpEntity<>(value, nestedHeaders));
                            } else {
                                nestedHeaders.setContentType(MediaType.TEXT_PLAIN);
                                multipartRequest
                                        .add(name, new HttpEntity<>(value.asText(), nestedHeaders));
                            }
                        }
                    });
                    return new HttpEntity<>(multipartRequest, httpHeaders);
                }).enrichHeaders(
                        h -> h.header(MessageHeaders.CONTENT_TYPE, MediaType.MULTIPART_FORM_DATA, true));
    }

    private RateLimiterRequestHandlerAdvice rateLimiterRequestHandlerAdvice() {
        return new RateLimiterRequestHandlerAdvice(RateLimiterConfig.custom()
                .timeoutDuration(Duration.ofSeconds(properties.getRequestPerSecond()))
                .limitRefreshPeriod(ONE_SECOND)
                .limitForPeriod(properties.getRequestPerSecond())
                .build());
    }

    @Bean
    public ResourceLoaderSupport resourceLoaderSupport() {
        return new ResourceLoaderSupport(resourceLoader, properties.getResourceLocationUri());
    }

    @Bean
    public DefaultHttpHeaderMapper defaultHttpHeaderMapper() {
        return new DefaultHttpHeaderMapper() {
            {
                DefaultHttpHeaderMapper.setupDefaultOutboundMapper(this);
                setInboundHeaderNames(properties.getMappedResponseHeaders());
                setOutboundHeaderNames(properties.getMappedRequestHeaders());
            }

            protected Object getHttpHeader(HttpHeaders source, String name) {
                if (ALLOW.equalsIgnoreCase(name)) {
                    String value = source.getFirst(ALLOW);
                    if (!StringUtils.isEmpty(value)) {
                        String[] tokens = StringUtils.tokenizeToStringArray(value, ",");
                        List<HttpMethod> result = new ArrayList<>(tokens.length);
                        for (String token : tokens) {
                            HttpMethod resolved = HttpMethod.resolve(token);
                            if (resolved != null) {
                                result.add(resolved);
                            }
                        }
                        return result;
                    } else {
                        return Collections.emptyList();
                    }
                } else {
                    return super.getHttpHeader(source, name);
                }
            }
        };
    }

    @Bean
    public IntegrationFlow httpErrorResponseFlow(MessageChannel errorChannel, DefaultHttpHeaderMapper headerMapper) {
        Set<Integer> retryStatusCodes = new HashSet<>(Arrays.asList(properties.getRetryErrorStatusCodes()));
        return IntegrationFlows.from(errorChannel)
                .<MessageHandlingException>handle((p, h) -> {
                    Message<?> failedMessage = p.getFailedMessage();
                    MessageHeaders failedMessageHeaders = failedMessage.getHeaders();
                    Throwable t = p.getMostSpecificCause();
                    if (t instanceof WebClientResponseException) {
                        WebClientResponseException exception = (WebClientResponseException) p.getMostSpecificCause();
                        HttpStatus statusCode = exception.getStatusCode();
                        if (retryStatusCodes.contains(statusCode.value())) {
                            return MessageBuilder.fromMessage(failedMessage)
                                    .setHeader(HTTP_STATUS_CODE_HEADER, statusCode.value())
                                    .build();
                        } else {
                            HttpHeaders httpHeaders = exception.getHeaders();
                            byte[] body = exception.getResponseBodyAsByteArray();
                            return MessageBuilder.withPayload(body)
                                    .copyHeaders(headerMapper.toHeaders(httpHeaders))
                                    .copyHeaders(failedMessageHeaders)
                                    .setHeader(HTTP_STATUS_CODE_HEADER, statusCode.value())
                                    .build();
                        }
                    } else {
                        return MessageBuilder.withPayload(p.getMostSpecificCause().getMessage())
                                .copyHeaders(failedMessageHeaders)
                                .setHeader(HTTP_STATUS_CODE_HEADER, HttpStatus.BAD_GATEWAY.value())
                                .build();
                    }
                }).route(Message.class,
                        m -> retryStatusCodes.contains(m.getHeaders().get(HTTP_STATUS_CODE_HEADER, Integer.class)),
                        m -> m.subFlowMapping(true, retryFlow -> retryFlow.publishSubscribeChannel(executorService(),
                                s -> s.subscribe(res -> res.channel(HttpclientGatewayProcessor.HTTP_ERROR_RESPONSE))
                                        .subscribe(res -> res.channel(ANALYTICS_FLOW_INPUT))))
                                .subFlowMapping(false, errorResponseFlow -> errorResponseFlow.publishSubscribeChannel(executorService(),
                                        s -> s.subscribe(res -> res.channel(HttpclientGatewayProcessor.OUTPUT))
                                                .subscribe(res -> res.channel(ANALYTICS_FLOW_INPUT))))).get();
    }

    public interface HttpclientGatewayProcessor extends Processor {

        String HTTP_ERROR_RESPONSE = "httpErrorResponse";

        String ANALYTICS = "analytics";

        @Output(HTTP_ERROR_RESPONSE)
        MessageChannel httpErrorResponse();

        @Output(ANALYTICS)
        MessageChannel analytics();
    }
}

