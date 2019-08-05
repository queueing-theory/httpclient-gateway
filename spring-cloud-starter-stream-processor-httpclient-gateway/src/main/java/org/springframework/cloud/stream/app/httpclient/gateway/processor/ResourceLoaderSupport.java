package org.springframework.cloud.stream.app.httpclient.gateway.processor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.WritableResource;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.MediaType;
import org.springframework.util.Assert;
import org.springframework.web.util.UriComponentsBuilder;

class ResourceLoaderSupport {

    private ResourceLoader resourceLoader;

    private String location;

    ResourceLoaderSupport(ResourceLoader resourceLoader, String location) {
        Assert.isTrue(location.endsWith("/") ^ location.contains("{filename}"),
                "resourceLocationUri should either end with '/' or has a 'filename' variable");
        this.resourceLoader = resourceLoader;
        this.location = location;
    }

    private static Map<String, Object> createUriVariables(String name) {
        LocalDateTime localDateTime = LocalDateTime.now();
        Map<String, Object> variables = new HashMap<>();
        variables.put("uuid", UUID.nameUUIDFromBytes(name.getBytes()));
        variables.put("random_uuid", UUID.randomUUID().toString());
        variables.put("filename", Objects.requireNonNull(name));
        variables.put("yyyy", localDateTime.getYear());
        variables.put("MM", String.format("%02d", localDateTime.getMonthValue()));
        variables.put("dd", String.format("%02d", localDateTime.getDayOfMonth()));
        variables.put("HH", String.format("%02d", localDateTime.getHour()));
        variables.put("mm", String.format("%02d", localDateTime.getMinute()));
        variables.put("ss", String.format("%02d", localDateTime.getSecond()));
        return Collections.unmodifiableMap(variables);
    }

    public Resource externalizeAsResource(MediaType mediaType, DataBuffer dataBuffer) throws IOException {
        String name = UUID.nameUUIDFromBytes(mediaType.toString().getBytes()).toString();
        Resource resource = createResource(name);
        WritableResource writableResource = (WritableResource) resource;
        try (OutputStream outputStream = writableResource.getOutputStream();
                InputStream inputStream = dataBuffer.asInputStream()) {
            IOUtils.copy(inputStream, outputStream);
        }
        return resource;
    }

    public Resource createResource(String name) throws IOException {
        UriComponentsBuilder uriComponentsBuilder = UriComponentsBuilder.fromUriString(location);
        if (location.endsWith("/")) {
            uriComponentsBuilder.path(name);
        }
        String uriString = uriComponentsBuilder.buildAndExpand(createUriVariables(name)).toString();
        Resource resource = resourceLoader.getResource(uriString);
        if (!resource.exists() && resource.isFile()) {
            if (!resource.getFile().getParentFile().exists()) {
                Files.createDirectories(resource.getFile().getParentFile().toPath());
            }
        }
        return resource;
    }

    public Resource getResource(String location) {
        return resourceLoader.getResource(location);
    }
}
