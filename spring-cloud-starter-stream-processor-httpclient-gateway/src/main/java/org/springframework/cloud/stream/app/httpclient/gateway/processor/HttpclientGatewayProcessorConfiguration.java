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
import org.springframework.http.*;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.handler.LoggingHandler.Level;
import org.springframework.integration.handler.advice.RateLimiterRequestHandlerAdvice;
import org.springframework.integration.http.support.DefaultHttpHeaderMapper;
import org.springframework.integration.webflux.dsl.EnhancedWebFlux;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.*;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.netty.http.client.HttpClient;

import java.net.URI;
import java.time.Duration;
import java.util.*;

import static org.springframework.cloud.stream.app.httpclient.gateway.processor.LambdaExceptionHandler.throwsException;

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

    private static final String HTTP_STATUS_TEXT_HEADER = "http_statusText";

    private static final String CONTINUATION_ID_HEADER = "continuation_id";

    private static final String ORIGINAL_CONTENT_TYPE = "original_content_type";

    private static final Duration ONE_SECOND = Duration.ofSeconds(1);

    @Autowired
    private HttpclientGatewayProcessorProperties properties;
    @Autowired
    private ResourceLoader resourceLoader;
    @Autowired
    private ObjectMapper objectMapper;
    private PathMatcher pathMatcher = new AntPathMatcher();

    @Bean
    IntegrationFlow httpClientFlow(HttpclientGatewayProcessor processor,
                                   DefaultHttpHeaderMapper headerMapper, ResourceLoaderSupport resourceLoaderSupport) {

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
                .log(Level.INFO, m -> m)
                .headerFilter("host")
                .handle(EnhancedWebFlux.outboundGateway(m -> m.getHeaders().get(HTTP_REQUEST_URL_HEADER, String.class),
                        WebClient.builder().clientConnector(new ReactorClientHttpConnector(
                                        HttpClient.create().followRedirect(properties.isFollowRedirect())
                                                .wiretap(properties.isWireTap())
                                )
                        ).build())
                                .httpMethodFunction(m -> m.getHeaders().get(HTTP_REQUEST_METHOD_HEADER))
                                .headerMapper(headerMapper)
                                .bodyExtractorFunction(message -> {
                                    String httpRequestUrl = message.getHeaders().get(HTTP_REQUEST_URL_HEADER, String.class);

                                    return (inputMessage, context) -> {
                                        HttpHeaders httpHeaders = inputMessage.getHeaders();
                                        long contentLength = httpHeaders.getContentLength();
                                        Assert.notNull(contentLength, "Content-Length must not be null");
                                        return DataBufferUtils.join(inputMessage.getBody())
                                                .map(throwsException(buffer -> {
                                                    URI uri = URI.create(httpRequestUrl);
                                                    if (matchesUrlPatterns(uri.getPath()) ||
                                                            contentLength > properties.getContentLengthToExternalize()) {
                                                        MediaType mediaType = httpHeaders.getContentType();
                                                        Resource resource = resourceLoaderSupport
                                                                .externalizeAsResource(uri.getHost() + uri.getPath(), mediaType, buffer);
                                                        DataBufferUtils.release(buffer);
                                                        return resource;
                                                    } else {
                                                        byte[] bytes = new byte[buffer.readableByteCount()];
                                                        buffer.read(bytes);
                                                        DataBufferUtils.release(buffer);
                                                        return bytes;
                                                    }
                                                }));
                                    };
                                })
                        , c -> c.advice(rateLimiterRequestHandlerAdvice()))
                .<Object, Class<?>>route(object -> object instanceof Resource ? Resource.class : object.getClass(),
                        r -> r.subFlowMapping(Resource.class, sf -> sf.gateway(resourceExternalized()))
                                .subFlowMapping(byte[].class,
                                        sf -> sf.handle((p, h) -> p))
                )
                .log(Level.INFO, m -> m)
                .channel(processor.output()).get();
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

    private IntegrationFlow resourceExternalized() {
        return f -> f.enrichHeaders(h -> h.header("is_reference", true))
                .<Resource>handle(throwsException((p, h) -> {
                    String requestUrl = h.get(HTTP_REQUEST_URL_HEADER, String.class);
                    ObjectNode objectNode = objectMapper.createObjectNode();
                    objectNode.put(HTTP_REQUEST_URL_HEADER, requestUrl);
                    objectNode.put(ORIGINAL_CONTENT_TYPE, h.get(MessageHeaders.CONTENT_TYPE).toString());
                    objectNode.put("uri", p.getURI().toString());
                    return objectNode;
                })).enrichHeaders(
                        h -> h.header(MessageHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_UTF8, true));
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
                    Throwable t = p.getMostSpecificCause();
                    String continuationId = p.getFailedMessage().getHeaders().get(CONTINUATION_ID_HEADER, String.class);
                    if (t instanceof WebClientResponseException) {
                        WebClientResponseException exception = (WebClientResponseException) p.getMostSpecificCause();
                        int statusCode = exception.getRawStatusCode();
                        String statusText = exception.getStatusText();
                        if (retryStatusCodes.contains(statusCode)) {
                            return MessageBuilder.fromMessage(p.getFailedMessage())
                                    .setHeader(HTTP_STATUS_CODE_HEADER, statusCode)
                                    .setHeader(HTTP_STATUS_TEXT_HEADER, statusText)
                                    .build();
                        } else {
                            HttpHeaders httpHeaders = exception.getHeaders();
                            byte[] body = exception.getResponseBodyAsByteArray();
                            return MessageBuilder.withPayload(body)
                                    .setHeader(CONTINUATION_ID_HEADER, continuationId)
                                    .setHeader(HTTP_STATUS_CODE_HEADER, statusCode)
                                    .copyHeaders(headerMapper.toHeaders(httpHeaders))
                                    .build();
                        }
                    } else {
                        return MessageBuilder.withPayload(p.getMostSpecificCause().getMessage())
                                .setHeader(CONTINUATION_ID_HEADER, continuationId)
                                .setHeader(HTTP_STATUS_CODE_HEADER, 502)
                                .build();
                    }
                }).route(Message.class,
                        m -> retryStatusCodes.contains(m.getHeaders().get(HTTP_STATUS_CODE_HEADER, Integer.class)),
                        m -> m.channelMapping(true, HttpclientGatewayProcessor.HTTP_ERROR_RESPONSE)
                                .channelMapping(false, HttpclientGatewayProcessor.OUTPUT)).get();
    }

    public interface HttpclientGatewayProcessor extends Processor {

        String HTTP_ERROR_RESPONSE = "httpErrorResponse";

        @Output(HTTP_ERROR_RESPONSE)
        MessageChannel httpErrorResponse();
    }
}

