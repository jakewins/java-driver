package org.neo4j.driver.internal.connector.select;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

/**
 * Whenever something happens on a network connection (a packet is available to read, for instance), a background thread gets notified.
 * That background thread will get access to an instance of this class associated with the network connection something happened on, and it
 * will forward the event to here.
 */
public class SelectorCallback
{
    private final BoltV1AsyncDechunker dechunker;

    public SelectorCallback( BoltV1AsyncDechunker dechunker )
    {
        this.dechunker = dechunker;
    }

    /** At least one byte is available to read from the network */
    public void onIncomingNetworkData( ReadableByteChannel ch ) throws IOException
    {
        dechunker.handle( ch );
    }
}
