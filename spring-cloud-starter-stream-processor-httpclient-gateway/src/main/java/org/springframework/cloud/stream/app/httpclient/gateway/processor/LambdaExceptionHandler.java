package org.springframework.cloud.stream.app.httpclient.gateway.processor;

import java.util.function.Consumer;
import java.util.function.Function;
import org.springframework.integration.handler.GenericHandler;
import org.springframework.messaging.MessageHeaders;

class LambdaExceptionHandler {

    static <T, E extends Exception> Consumer<T> throwsException(
            ThrowingConsumer<T, E> throwingConsumer) {
        return i -> {
            try {
                throwingConsumer.accept(i);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        };
    }

    static <S, T, E extends Exception> Function<S, T> throwsException(
            ThrowingFunction<S, T, E> throwingFunction) {
        return i -> {
            try {
                return throwingFunction.apply(i);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        };
    }

    static <P, E extends Exception> GenericHandler<P> throwsException(
            ThrowingGenericHandler<P, E> throwingGenericHandler) {
        return (i, j) -> {
            try {
                return throwingGenericHandler.apply(i, j);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        };
    }

    @FunctionalInterface
    interface ThrowingConsumer<T, E extends Exception> {

        void accept(T t) throws E;
    }

    @FunctionalInterface
    interface ThrowingFunction<S, T, E extends Exception> {

        T apply(S s) throws E;
    }

    @FunctionalInterface
    public interface ThrowingGenericHandler<P, E extends Exception> {

        Object apply(P payload, MessageHeaders headers) throws E;

    }
}

