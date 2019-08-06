package org.springframework.cloud.stream.app.httpclient.gateway.processor;

import javax.validation.constraints.AssertTrue;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.http.support.DefaultHttpHeaderMapper;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration properties for the Http Client Processor module.
 *
 * @author Waldemar Hummer
 * @author Mark Fisher
 * @author Christian Tzolov
 * @author Artem Bilan
 * @author Haruhiko Nishi
 */
@ConfigurationProperties("httpclient-gateway")
@Validated
public class HttpclientGatewayProcessorProperties {

    /**
     * The URL to issue an http request to, as a static value.
     */
    private String url;

    /**
     * A SpEL expression against incoming message to determine the URL to use.
     */
    private Expression urlExpression;

    /**
     * Enable wireTap
     */
    private boolean wireTap = false;

    /**
     * HTTP Status Codes to retry
     */
    private Integer[] retryErrorStatusCodes = {429};

    /**
     * Delay before HTTP requests are actually sent. Works as preemptive rate limit.
     */
    private Integer requestPerSecond = 1;

    /**
     * Http Request Headers that will be mapped.
     */
    private String[] mappedRequestHeaders = {DefaultHttpHeaderMapper.HTTP_REQUEST_HEADER_NAME_PATTERN,
            "Origin",
            "Access-Control-Request-Method",
            "Access-Control-Request-Headers",
            "X-*"
    };

    /**
     * Http Response Headers that will be mapped.
     */
    private String[] mappedResponseHeaders = {DefaultHttpHeaderMapper.HTTP_RESPONSE_HEADER_NAME_PATTERN,
            "Access-Control-Allow-Origin",
            "Access-Control-Expose-Headers",
            "Access-Control-Max-Age",
            "Access-Control-Allow-Credentials",
            "Access-Control-Allow-Methods",
            "Access-Control-Allow-Headers",
            "X-*"
    };

    /**
     * Follow Redirects
     */
    private boolean followRedirect = true;

    /**
     * Base URI where externalized contents will be stored.
     */
    private String resourceLocationUri = "file:///tmp";

    /**
     * URL patterns to determine whether content downloaded from remote servers should be stored at resourceLocationUri
     */
    private String[] urlPatternsToExternalize;

    /**
     * Threshold values that determines when content downloaded from remote servers should be stored at resourceLocationUri.
     */
    private long contentLengthToExternalize = 1024;

    public String getUrl() {
        return this.url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Expression getUrlExpression() {
        return urlExpression != null ? urlExpression
                : new LiteralExpression(this.url);
    }

    public void setUrlExpression(Expression urlExpression) {
        this.urlExpression = urlExpression;
    }

    public boolean isWireTap() {
        return this.wireTap;
    }

    public void setWireTap(boolean wireTap) {
        this.wireTap = wireTap;
    }

    @AssertTrue(message = "Exactly one of 'url' or 'urlExpression' is required")
    public boolean isExactlyOneUrl() {
        return url == null ^ urlExpression == null;
    }

    public Integer[] getRetryErrorStatusCodes() {
        return retryErrorStatusCodes;
    }

    public void setRetryErrorStatusCodes(Integer[] retryErrorStatusCodes) {
        this.retryErrorStatusCodes = retryErrorStatusCodes;
    }

    public String[] getMappedResponseHeaders() {
        return mappedResponseHeaders;
    }

    public void setMappedResponseHeaders(String[] mappedResponseHeaders) {
        this.mappedResponseHeaders = mappedResponseHeaders;
    }

    public String[] getMappedRequestHeaders() {
        return mappedRequestHeaders;
    }

    public void setMappedRequestHeaders(String[] mappedRequestHeaders) {
        this.mappedRequestHeaders = mappedRequestHeaders;
    }

    public boolean isFollowRedirect() {
        return followRedirect;
    }

    public void setFollowRedirect(boolean followRedirect) {
        this.followRedirect = followRedirect;
    }

    public Integer getRequestPerSecond() {
        return requestPerSecond;
    }

    public void setRequestPerSecond(Integer requestPerSecond) {
        this.requestPerSecond = requestPerSecond;
    }

    public long getContentLengthToExternalize() {
        return contentLengthToExternalize;
    }

    public void setContentLengthToExternalize(long contentLengthToExternalize) {
        this.contentLengthToExternalize = contentLengthToExternalize;
    }

    public String getResourceLocationUri() {
        return resourceLocationUri;
    }

    public void setResourceLocationUri(String resourceLocationUri) {
        this.resourceLocationUri = resourceLocationUri;
    }

    public String[] getUrlPatternsToExternalize() {
        return urlPatternsToExternalize;
    }

    public void setUrlPatternsToExternalize(String[] urlPatternsToExternalize) {
        this.urlPatternsToExternalize = urlPatternsToExternalize;
    }
}
