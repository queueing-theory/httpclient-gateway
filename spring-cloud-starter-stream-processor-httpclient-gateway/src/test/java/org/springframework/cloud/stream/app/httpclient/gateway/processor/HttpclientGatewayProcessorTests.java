package org.springframework.cloud.stream.app.httpclient.gateway.processor;

import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.springframework.cloud.stream.test.matcher.MessageQueueMatcher.receivesPayloadThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.gaul.httpbin.HttpBin;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cloud.stream.app.httpclient.gateway.processor.HttpclientGatewayProcessorConfiguration.HttpclientGatewayProcessor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration(initializers = HttpclientGatewayProcessorTests.Initializer.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
public abstract class HttpclientGatewayProcessorTests {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final byte[] LARGE_BYTE_ARRAY = new byte[1_000_000];

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    protected HttpclientGatewayProcessor channels;

    @Autowired
    protected MessageCollector messageCollector;

    @TestPropertySource(properties = {"httpclient-gateway.requestPerSecond=1",
            "httpclient-gateway.resourceLocationUri=file://tmp/",
            "httpclient-gateway.contentLengthToExternalize=1000000"})
    public static class DefaultHttpclientGatewayProcessorTests extends HttpclientGatewayProcessorTests {

        @Test
        public void testHttpClientGatewayProcessor() {
            Map<String, Object> map = new HashMap<>();
            map.put("http_requestMethod", "GET");
            map.put("http_requestUrl", "http://some.domain/get?foo=1&bar=2");
            MessageHeaders messageHeaders = new MessageHeaders(map);
            Message message = MessageBuilder.createMessage(EMPTY_BYTE_ARRAY, messageHeaders);
            channels.input().send(message);
            assertThat(messageCollector.forChannel(channels.output()),
                    receivesPayloadThat(allOf(
                            hasJsonPath("$.url"),
                            hasJsonPath("$.headers.User-Agent", is("ReactorNetty/0.8.5.RELEASE")),
                            hasJsonPath("$.origin"),
                            hasJsonPath("$.args.foo", is("1")),
                            hasJsonPath("$.args.bar", is("2"))
                    )));
        }

        @Test
        public void testHttpStatus429() throws Exception {
            Map<String, Object> map = new HashMap<>();
            map.put("http_requestMethod", "GET");
            map.put("http_requestUrl", "http://some.domain/status/429");
            MessageHeaders messageHeaders = new MessageHeaders(map);
            Message message = MessageBuilder.createMessage(EMPTY_BYTE_ARRAY, messageHeaders);
            channels.input().send(message);
            Message<?> messageToRetry = messageCollector.forChannel(channels.httpErrorResponse()).take();
            assertThat(messageToRetry.getHeaders().get("http_statusCode"), is(429));
            assertThat(messageToRetry.getHeaders().get("http_statusText"), is("Too Many Requests"));
        }

        @Test
        public void testLargeResponseBodyExternalized() throws Exception {
            Map<String, Object> map = new HashMap<>();
            map.put("http_requestMethod", "POST");
            map.put("http_requestUrl", "http://some.domain/post");
            MessageHeaders messageHeaders = new MessageHeaders(map);
            Message message = MessageBuilder.createMessage(LARGE_BYTE_ARRAY, messageHeaders);
            channels.input().send(message);
            assertThat(messageCollector.forChannel(channels.output()),
                    receivesPayloadThat(allOf(
                            hasJsonPath("$.http_requestUrl"),
                            hasJsonPath("$.original_content_type", is("application/json"))
                    )));
        }

        @Test
        public void testMultiPart() throws Exception {
            Map<String, Object> map = new HashMap<>();
            map.put(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
            map.put("http_requestMethod", "POST");
            map.put("original_content_type", "multipart/form-data");
            map.put("http_requestUrl", "http://some.domain/post");
            String payload = "[{\"some-number\":4},{\"some-json-array\":[\"4\"]},"
                    + "{\"formParameterName\":\"data1\",\"originalFileName\":\"filename.txt\",\"contentType\":\"text/plain\",\"uri\":\"classpath:128b_file\"},"
                    + "{\"formParameterName\":\"data2\",\"originalFileName\":\"other-file-name.data\",\"contentType\":\"text/plain\",\"uri\":\"classpath:128b_file\"},"
                    + "{\"formParameterName\":\"json\",\"originalFileName\":\"test.json\",\"contentType\":\"application/json\",\"uri\":\"classpath:128b_file\"}]";
            JsonNode jsonNode = objectMapper.readTree(payload);
            MessageHeaders messageHeaders = new MessageHeaders(map);
            Message message = MessageBuilder.createMessage(jsonNode, messageHeaders);
            channels.input().send(message);
            assertThat(messageCollector.forChannel(channels.output()),
                    receivesPayloadThat(allOf(
                            hasJsonPath("$.data.some-json-array"),
                            hasJsonPath("$.data.data2"),
                            hasJsonPath("$.data.data1"),
                            hasJsonPath("$.data.json"),
                            hasJsonPath("$.data.some-number")
                    )));
        }

    }

    static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext>,
            ApplicationListener<ContextClosedEvent> {

        URI httpBinEndpoint = URI.create("http://127.0.0.1:0");

        HttpBin httpBin;

        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            try {
                httpBin = new HttpBin(httpBinEndpoint);
                httpBin.start();
                URI uri = new URI(httpBinEndpoint.getScheme(),
                        httpBinEndpoint.getUserInfo(), httpBinEndpoint.getHost(),
                        httpBin.getPort(), httpBinEndpoint.getPath(),
                        httpBinEndpoint.getQuery(), httpBinEndpoint.getFragment());
                TestPropertyValues.of("httpclient-gateway.url=" + uri.toString())
                        .applyTo(configurableApplicationContext.getEnvironment());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onApplicationEvent(ContextClosedEvent event) {
            try {
                httpBin.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @SpringBootApplication
    public static class DefaultHttpclientGatewayProcessorApplication {

    }
}
