package org.neo4j.driver.internal.connector.select;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.LinkedList;

import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.util.ThrowingConsumer;

/**
 * Bolt V1 dechunking implementation that handles chunks split into arbitrary network packets. As data becomes available, the dechunker will forward
 * complete messages for deserialization.
 *
 * In a later release, we'd ideally do direct incremental deserialization, rather than build up the whole serialized message in RAM - and in a later version
 * of Bolt, we'd ideally avoid the deserialization altogether, evolving PackStream to work more like FlatBuffers, SBE and similar.
 *
 * This class is not thread safe, there's one of these per connection to the database.
 */
public class BoltV1IncrementalDechunker
{
    private static final int mainBufferSize = Integer.getInteger( "xx.neo4j.driver.deserialization_buffer.mainSize", 1024 * 4 );
    private static final int expandBufferSize = Integer.getInteger( "xx.neo4j.driver.deserialization_buffer.expandSize", 1024 * 2 );

    private final ThrowingConsumer<PackInput,IOException> onMessage;
    private final ByteBuffer header = ByteBuffer.allocateDirect( 2 );

    /** Exposes {@link #messageBuffer} and {@link #expansionBuffers} as a uniform {@link PackInput} */
    private final DechunkedBufferView bufferView;

    /** This is the buffer we write the dechunked binary message data to.. as long as it fits in here */
    private final ByteBuffer messageBuffer;

    /** This is a pointer to whatever buffer we're currently reading into, normally {@link #messageBuffer}, but could also be head({@link #expansionBuffers}) */
    private ByteBuffer currentBuffer;

    /**
     * So, here's the deal. The {@link #messageBuffer} above is meant to be, for 99% of messages, the allocate-at-startup go-to buffer for deserializing inbound
     * messages. We read the dechunked binary data into that buffer. However, there is no boundary for the message size - the user may fetch a very large
     * string at some point, and we'd prefer to not use up a bunch of RAM "just in case".
     *
     * At the same time, we'd also prefer to not use a complicated pooling solution like Netty had to do. So, what do we do? Well, the list below contains
     * temporary "expansion" buffers, which we allocate dynamically when we've filled {@link #messageBuffer}. Once the unusually large message is deserialized,
     * these expansion buffers are released, and GC cleans them up.
     *
     * This design does create some GC churn - but it should be minimal, and it's super easy to make the size of the main buffer configurable such that
     * applications with lots of large messages can have a large fixed messageBuffer.
     */
    private final LinkedList<ByteBuffer> expansionBuffers = new LinkedList<>();

    private int remainingInChunk = 0;

    private State state = State.AWAITING_HEADER;

    private boolean currentChannelHasMore = true;
    private ReadableByteChannel currentChannel;

    public enum State
    {
        /**
         * We are waiting for the first or the second header byte. This state will write those arriving header bytes to {@link #header},
         * and once we have both will read the header and dispatch either to {@link #messageComplete()} or move us to {@link #IN_CHUNK}.
         */
        AWAITING_HEADER
        {
            @Override
            State handle( BoltV1IncrementalDechunker ctx ) throws IOException
            {
                ByteBuffer headerBuffer = ctx.header;

                ctx.read( headerBuffer );
                if( headerBuffer.remaining() == 0 )
                {
                    // Read the complete two-byte header
                    headerBuffer.flip();
                    int chunkSize = headerBuffer.getShort() & 0xFFFF;
                    headerBuffer.clear();
                    if(chunkSize == 0)
                    {
                        // Chunk size 0 means message boundary
                        ctx.messageComplete();
                        return AWAITING_HEADER;
                    }
                    else
                    {
                        ctx.remainingInChunk = chunkSize;
                        return IN_CHUNK;
                    }
                }
                else
                {
                    // Partial header, wait for the remaining byte
                    return AWAITING_HEADER;
                }
            }
        },

        /**
         * We are currently reading a chunk. Remember that a chunk can have gotten split up on the network, so we may be in this state for several
         * iterations as the chunk arrives in partial packets. As the chunk parts arrive, we append the chunk contents to our message buffer, which
         * will then end up containing a clean view of the original message.
         *
         * When we've recieved the whole chunk (tracked by {@link #remainingInChunk}, we return to waiting for a header in the {@link #AWAITING_HEADER} state.
         */
        IN_CHUNK
        {
            @Override
            State handle( BoltV1IncrementalDechunker ctx ) throws IOException
            {
                if( ctx.currentBuffer.position() < ctx.currentBuffer.capacity() )
                {
                    ctx.currentBuffer.limit( Math.min(
                            // Either the max we can fit in the buffer
                            ctx.currentBuffer.capacity(),

                            // Or the max we have left to read for the current chunk
                            ctx.currentBuffer.position() + ctx.remainingInChunk) );

                    ctx.remainingInChunk -= ctx.read( ctx.currentBuffer );

                    if( ctx.remainingInChunk == 0 )
                    {
                        return AWAITING_HEADER;
                    }
                    else
                    {
                        return IN_CHUNK;
                    }
                }
                throw new UnsupportedOperationException();
            }
        },
        CLOSED
        {
            @Override
            State handle( BoltV1IncrementalDechunker ctx )
            {
                return CLOSED;
            }
        };

        abstract State handle( BoltV1IncrementalDechunker ctx ) throws IOException;
    }

    private void messageComplete() throws IOException
    {
        messageBuffer.flip();
        onMessage.accept( bufferView );
        messageBuffer.clear();
        currentBuffer = messageBuffer;
        // TODO clear expand buffers
    }

    private int read( ByteBuffer target ) throws IOException
    {
        int read = currentChannel.read( target );
        if(read <= 0)
        {
            currentChannelHasMore = false;
        }
        return read;
    }

    /**
     * @param onMessage called with a complete dechunked message available.
     */
    public BoltV1IncrementalDechunker( ThrowingConsumer<PackInput,IOException> onMessage )
    {
        this.onMessage = onMessage;
        this.messageBuffer = currentBuffer = ByteBuffer.allocateDirect( mainBufferSize );
        this.bufferView = new DechunkedBufferView( messageBuffer );
    }

    void handle( ReadableByteChannel ch ) throws IOException
    {
        currentChannelHasMore = true;
        currentChannel = ch;
        while( currentChannelHasMore )
        {
            state = this.state.handle( this );
        }
    }
}

class DechunkedBufferView implements PackInput
{
    private final ByteBuffer messageBuffer;

    DechunkedBufferView( ByteBuffer messageBuffer )
    {
        this.messageBuffer = messageBuffer;
    }

    @Override
    public boolean hasMoreData() throws IOException
    {
        return messageBuffer.hasRemaining();
    }

    @Override
    public byte readByte() throws IOException
    {
        if( messageBuffer.hasRemaining() )
        {
            return messageBuffer.get();
        }
        throw new IOException( "Read past end of message, expected at least one more byte." );
    }

    @Override
    public short readShort() throws IOException
    {
        if(messageBuffer.remaining() >= 2)
        {
            return messageBuffer.getShort();
        }
        throw new IOException( "Read past end of message, expected at least two more bytes." );
    }

    @Override
    public int readInt() throws IOException
    {
        if(messageBuffer.remaining() >= 4)
        {
            return messageBuffer.getInt();
        }
        throw new IOException( "Read past end of message, expected at least four more bytes." );
    }

    @Override
    public long readLong() throws IOException
    {
        if(messageBuffer.remaining() >= 8)
        {
            return messageBuffer.getLong();
        }
        throw new IOException( "Read past end of message, expected at least eight more bytes." );
    }

    @Override
    public double readDouble() throws IOException
    {
        if(messageBuffer.remaining() >= 8)
        {
            return messageBuffer.getDouble();
        }
        throw new IOException( "Read past end of message, expected at least eight more bytes." );
    }

    @Override
    public PackInput readBytes( byte[] into, int offset, int toRead ) throws IOException
    {
        if(messageBuffer.remaining() >= toRead)
        {
            messageBuffer.get( into, offset, toRead );
            return this;
        }
        throw new IOException( "Read past end of message, expected at least "+toRead+" more bytes." );
    }

    @Override
    public byte peekByte() throws IOException
    {
        if( messageBuffer.hasRemaining() )
        {
            return messageBuffer.get(messageBuffer.position() + 1);
        }
        throw new IOException( "Read past end of message, expected at least one more byte." );
    }
}