package org.neo4j.driver.internal.connector.select;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.net.URI;

import org.neo4j.driver.Config;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.util.TestNeo4j;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class SelectorConnectionIT
{
    @Rule
    public TestNeo4j neo4j = new TestNeo4j();
    private Connection connection;

    @Test
    public void shouldHandleBasicStream() throws Throwable
    {
        // Given
        SelectorConnector connector = new SelectorConnector();
        connection = connector.connect( new URI( neo4j.address() ), Config.defaultConfig() );

        // When
        BoltV1AsyncResultCollector result = new BoltV1AsyncResultCollector();
        connection.run( "RETURN 1 AS x", Values.parameters(), result );
        connection.pullAll( result );
        connection.sync();

        // Then
        assertThat( result.next().get( 0 ), equalTo( Values.value( 1 ) ));
        assertThat( result.next(), is( nullValue() ) );
    }

    @After
    public void cleanup()
    {
        connection.close();
    }

}