package org.neo4j.driver.internal.connector.select;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.connector.socket.ChunkedOutput;
import org.neo4j.driver.internal.messaging.MessageHandler;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.StreamCollector;

public class SelectorConnection implements Connection
{

    private final MessageHandler output;

    public SelectorConnection( Selector selector, String host, int port ) throws ClientException
    {
        try
        {
            // Set up socket
            SocketChannel soChannel = SocketChannel.open();
            soChannel.setOption( StandardSocketOptions.SO_REUSEADDR, true );
            soChannel.setOption( StandardSocketOptions.SO_KEEPALIVE, true );
            soChannel.connect( new InetSocketAddress( host, port ) );
            soChannel.configureBlocking( false );
            SelectionKey key = soChannel.register( selector, SelectionKey.OP_READ );
            key.attach( new SelectorCallback( new BoltV1IncrementalDechunker( null ) ) );

            // Set out messaging output
            output = new PackStreamMessageFormatV1.Writer( new ChunkedOutput( 1400, soChannel ) );
        }
        catch ( IOException e )
        {
            throw new ClientException( "Unable to establish connection: " + e.getMessage(), e );
        }
    }

    @Override
    public void init( String clientName )
    {
        try
        {
            output.handleInitMessage( clientName );
        }
        catch ( IOException e )
        {
            throw new ClientException( "Failed to write INIT message to network: " + e.getMessage(), e );
        }
    }

    @Override
    public void run( String statement, Map<String,Value> parameters, StreamCollector collector )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void discardAll()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void pullAll( StreamCollector collector )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException();
    }
}
