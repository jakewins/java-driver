package org.neo4j.driver.internal.connector.select;

import java.net.URI;
import java.nio.channels.Selector;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.driver.Config;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.Version;
import org.neo4j.driver.internal.connector.socket.SocketConnector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.spi.Logger;

import static java.util.Collections.singletonList;

public class SelectorConnector implements Connector, AutoCloseable
{
    // TODO: This should be per-driver-instance, and the size of it should be configurable!
    private final ConcurrentHashMap<URI, SelectorPool> selectors = new ConcurrentHashMap<>();

    @Override
    public Connection connect( URI sessionURI, Config config ) throws ClientException
    {
        int port = sessionURI.getPort() == -1 ? SocketConnector.DEFAULT_PORT : sessionURI.getPort();
        Selector selector = selectorFor( sessionURI, config.logging().getLog( SelectorConnector.class.getName() ) );

        SelectorConnection conn = new SelectorConnection( selector,  sessionURI.getHost(), port );
        conn.init( "bolt-java-driver/" + Version.driverVersion() );
        return conn;
    }

    @Override
    public void close() throws Exception
    {
        for ( SelectorPool selectorPool : selectors.values() )
        {
            selectorPool.close();
        }
    }

    private Selector selectorFor( URI sessionURI, Logger log )
    {
        SelectorPool selectorPool = selectors.get( sessionURI );
        if(selectorPool == null)
        {
            // TODO: We need a way to clean these threads up when the driver is closed
            // TODO: configurable size

            // Really, this state should live with the driver instance in some way - either in the config object or in some state holder passed in to the
            // connect method above.
            selectorPool = new SelectorPool(2, log);
            selectors.put( sessionURI, selectorPool );
        }
        return selectorPool.assignSelector();
    }

    @Override
    public Collection<String> supportedSchemes()
    {
        return singletonList( SocketConnector.SCHEME );
    }
}
