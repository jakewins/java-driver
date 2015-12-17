package org.neo4j.driver.internal.connector.select;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Map;

import org.neo4j.driver.internal.connector.socket.ChunkedOutput;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.StreamCollector;
import org.neo4j.driver.internal.util.ThrowingConsumer;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

public class SelectorConnection implements Connection
{
    private final PackStreamMessageFormatV1.Writer output;
    private final PackStreamMessageFormatV1.Reader input;
    private final BoltV1MessageRouter router;
    private final SelectionKey key;
    private final SocketChannel channel;

    public SelectorConnection( Selector selector, String host, int port ) throws ClientException
    {
        try
        {
            // Set up socket
            channel = connect( host, port );
            key = channel.register( selector, SelectionKey.OP_READ );
            key.attach( new SelectorCallback( new BoltV1AsyncDechunker( new ThrowingConsumer<PackInput,IOException>()
            {
                @Override
                public void accept( PackInput packInput ) throws IOException
                {
                    input.reset( packInput ).read( router );
                }
            } ) ) );

            // Set out messaging output
            router = new BoltV1MessageRouter();
            output = new PackStreamMessageFormatV1.Writer( new ChunkedOutput( 1400, channel ) );
            input = new PackStreamMessageFormatV1.Reader( null, new Runnable()
            {
                @Override
                public void run()
                {
                    
                }
            } );
        }
        catch ( IOException e )
        {
            throw new ClientException( "Unable to establish connection: " + e.getMessage(), e );
        }
    }

    private SocketChannel connect( String host, int port ) throws IOException
    {
        SocketChannel channel = SocketChannel.open();
        channel.setOption( StandardSocketOptions.SO_REUSEADDR, true );
        channel.setOption( StandardSocketOptions.SO_KEEPALIVE, true );
        channel.connect( new InetSocketAddress( host, port ) );

        // Version negotiation
        ByteBuffer protocolNegotiation = ByteBuffer.allocate( 16 );
        protocolNegotiation.putInt( 1 );
        protocolNegotiation.position( 0 );

        channel.write( protocolNegotiation );

        protocolNegotiation.clear();
        channel.read( protocolNegotiation );
        protocolNegotiation.flip();

        int chosenVersion = protocolNegotiation.getInt();
        if( chosenVersion != 1 )
        {
            throw new ClientException( "Database does not support any of the protocol versions supported by this driver. Please refer to the driver documentation" +
                                       "to choose a driver that is compatible with your database." );
        }

        channel.configureBlocking( false );

        return channel;
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
        try
        {
            output.handleRunMessage( statement, parameters );
            router.register( collector );
        }
        catch ( IOException e )
        {
            throw new ClientException( "Failed to write RUN message to network: " + e.getMessage(), e );
        }
    }

    @Override
    public void discardAll()
    {
        try
        {
            output.handleDiscardAllMessage();
        }
        catch ( IOException e )
        {
            throw new ClientException( "Failed to write DISCARD_ALL message to network: " + e.getMessage(), e );
        }
    }

    @Override
    public void pullAll( StreamCollector collector )
    {
        try
        {
            output.handlePullAllMessage();
        }
        catch ( IOException e )
        {
            throw new ClientException( "Failed to write PULL_ALL message to network: " + e.getMessage(), e );
        }
    }

    @Override
    public void sync()
    {
        try
        {
            output.flush();
        }
        catch ( IOException e )
        {
            throw new ClientException( "Failed to write outbound data to network: " + e.getMessage(), e );
        }
    }

    @Override
    public void close()
    {
        key.cancel();
        try
        {
            channel.close();
        }
        catch ( IOException e )
        {
            throw new ClientException( "Failed to close network connection: " + e.getMessage(), e );
        }
    }
}
