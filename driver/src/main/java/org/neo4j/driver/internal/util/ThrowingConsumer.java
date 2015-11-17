package org.neo4j.driver.internal.util;

public interface ThrowingConsumer<T, EXCEPTION extends Exception>
{
    void accept(T t) throws EXCEPTION;
}
