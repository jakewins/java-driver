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
public class BoltV1AsyncDechunker
{
    public static final int defaultMainBufferSize = Integer.getInteger( "xx.neo4j.driver.deserialization_buffer.mainSize", 1024 * 4 );
    private static final int defaultExpansionBufferSize = Integer.getInteger( "xx.neo4j.driver.deserialization_buffer.expandSize", 1024 * 2 );

    /** Size used to allocate temporary space when the {@link #mainBuffer} cannot fit the whole message */
    private final int expansionBufferSize;

    /** Once a complete message has been assembled, it is forwarded here */
    private final ThrowingConsumer<PackInput,IOException> onMessage;

    /** Used to read the 2-byte chunk header */
    private final ByteBuffer header = ByteBuffer.allocateDirect( 2 );

    /** Exposes {@link #dechunkedBuffers} as a uniform {@link PackInput} */
    private final DechunkedBufferView bufferView;

    /** This is the buffer we write the dechunked binary message data to.. as long as it fits in here */
    private final ByteBuffer mainBuffer;

    /** This is a pointer to whatever buffer we're currently reading into, normally {@link #mainBuffer}, but could also be head({@link #dechunkedBuffers}) */
    private ByteBuffer currentBuffer;

    /**
     * So, here's the deal. The {@link #mainBuffer} above is meant to be, for 99% of messages, the allocate-at-startup go-to buffer for deserializing inbound
     * messages. We read the dechunked binary data into that buffer. However, there is no boundary for the message size - the user may fetch a very large
     * string at some point, and we'd prefer to not use up a bunch of RAM "just in case".
     *
     * At the same time, we'd also prefer to not use a complicated pooling solution like Netty had to do. So, what to do? Well, we allow dynamic allocation
     * of on-heap buffers. All the buffers (including {@link #mainBuffer} go in this list, which is then used by {@link DechunkedBufferView} to expose the
     * dechunked message. For most messages, the {@link #mainBuffer} will be the only buffer in the list - but if the message is too large to fit, we append
     * dynamically allocated buffers behind it in this list.
     */
    private final LinkedList<ByteBuffer> dechunkedBuffers = new LinkedList<>();

    /** Number of bytes left to collect for the current chunk */
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
            State handle( BoltV1AsyncDechunker ctx ) throws IOException
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
            State handle( BoltV1AsyncDechunker ctx ) throws IOException
            {
                if( ctx.currentBuffer.position() == ctx.currentBuffer.capacity() )
                {
                    // We've run out of space in the current buffer :( Need to allocate a temporary expansion buffer for this message.

                    // Note that we allocate an on-heap buffer here, rather than a direct buffer. Obviously a direct buffer is required to
                    // read off the network, but the network stack in Java has a thread-local off-heap buffer that will kick in to act as a bridge.
                    // Thus; these temporary buffers go on the heap, no off-heap memory is dynamically allocated.
                    ctx.currentBuffer = ByteBuffer.allocate( ctx.expansionBufferSize );
                    ctx.dechunkedBuffers.add( ctx.currentBuffer );
                }

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
        },
        CLOSED
        {
            @Override
            State handle( BoltV1AsyncDechunker ctx )
            {
                return CLOSED;
            }
        };

        abstract State handle( BoltV1AsyncDechunker ctx ) throws IOException;
    }

    /**
     * @param onMessage called with a complete dechunked message available.
     */
    public BoltV1AsyncDechunker( ThrowingConsumer<PackInput,IOException> onMessage )
    {
        this( onMessage, defaultMainBufferSize, defaultExpansionBufferSize );
    }

    /**
     * @param onMessage called with a complete dechunked message available.
     * @param mainBufferSize default buffer used for dechunking, this is allocated once and kept around permanently
     * @param expansionBufferSize buffer size for "expansion" buffers, allocated when message does not fit in main buffer
     */
    public BoltV1AsyncDechunker( ThrowingConsumer<PackInput,IOException> onMessage, int mainBufferSize, int expansionBufferSize )
    {
        this.onMessage = onMessage;
        this.expansionBufferSize = expansionBufferSize;

        this.mainBuffer = currentBuffer = ByteBuffer.allocateDirect( mainBufferSize );
        this.dechunkedBuffers.add( mainBuffer );

        this.bufferView = new DechunkedBufferView( dechunkedBuffers );
    }

    /** Main input method - whenever new data becomes available, it is published here for dechunking. */
    void handle( ReadableByteChannel ch ) throws IOException
    {
        currentChannelHasMore = true;
        currentChannel = ch;
        while( currentChannelHasMore )
        {
            state = state.handle( this );
        }
    }

    /** The current message we are collecting is complete. Expose it as a {@link org.neo4j.driver.internal.packstream.PackInput} to downstream subscriber */
    private void messageComplete() throws IOException
    {
        onMessage.accept( bufferView );

        // And then reset, prepping to collect the next message.
        mainBuffer.clear();
        bufferView.reset();

        if( dechunkedBuffers.size() > 0 )
        {
            dechunkedBuffers.clear();
        }

        // Main buffer is always present in the list of dechunked buffers
        dechunkedBuffers.add( mainBuffer );
        currentBuffer = mainBuffer;
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
}

/**
 * This class exposes a list of buffers as a single consecutive message. Mainly this means it deals with reading multi-byte values when they are split
 * across two separate buffers.
 */
class DechunkedBufferView implements PackInput
{
    // Floating points are complicated. Whenever they have overflown into multiple buffers, we read them as binary data into this
    // buffer, and then let the Stdlib decode them for us.
    private final ByteBuffer doubleDeserializationBuffer = ByteBuffer.allocateDirect( 8 );

    private final LinkedList<ByteBuffer> buffers;
    private ByteBuffer currentBuffer;

    DechunkedBufferView( LinkedList<ByteBuffer> buffers )
    {
        this.buffers = buffers;
    }

    @Override
    public boolean hasMoreData() throws IOException
    {
        return (currentBuffer != null && currentBuffer.hasRemaining()) || buffers.size() > 0;
    }

    @Override
    public byte readByte() throws IOException
    {
        ensureOne();
        return currentBuffer.get();
    }

    @Override
    public short readShort() throws IOException
    {
        if( currentBufferHas( 2 ) )
        {
            return currentBuffer.getShort();
        }
        else
        {
            // Value is split across buffers, fall back to more expensive option
            return (short) ((readByte()) << 8 | (readByte() & 0xFF));
        }
    }

    @Override
    public int readInt() throws IOException
    {
        if( currentBufferHas( 4 ) )
        {
            return currentBuffer.getInt();
        }
        else
        {
            // Value is split across buffers, fall back to more expensive option
            return (readShort()) << 16 | (readShort() & 0xFFFF);
        }
    }

    @Override
    public long readLong() throws IOException
    {
        if( currentBufferHas( 8 ) )
        {
            return currentBuffer.getLong();
        }
        else
        {
            // Value is split across buffers, fall back to more expensive option
            return ((long)readInt()) << 32 | (readInt() & 0xFFFFFFFFl);
        }
    }

    @Override
    public double readDouble() throws IOException
    {
        if( currentBufferHas( 8 ) )
        {
            return currentBuffer.getDouble();
        }
        else
        {
            doubleDeserializationBuffer.clear();
            for ( int i = 0; i < 8; i++ )
            {
                doubleDeserializationBuffer.put( readByte() );
            }
            doubleDeserializationBuffer.flip();
            return doubleDeserializationBuffer.getDouble();
        }
    }

    @Override
    public PackInput readBytes( byte[] into, int offset, int length ) throws IOException
    {
        if(currentBufferHas(length))
        {
            currentBuffer.get( into, offset, length );
            return this;
        }
        else
        {
            int toRead = currentBuffer.remaining();
            currentBuffer.get( into, offset, currentBuffer.remaining() );
            return readBytes( into, offset + toRead, length - toRead );
        }
    }

    @Override
    public byte peekByte() throws IOException
    {
        ensureOne();
        return currentBuffer.get(currentBuffer.position() + 1);
    }

    public void reset()
    {
        currentBuffer = null;
    }

    private boolean currentBufferHas( int remaining )
    {
        ensureOne();
        return currentBuffer.remaining() >= remaining;
    }

    private void ensureOne()
    {
        if(currentBuffer == null || currentBuffer.remaining() == 0)
        {
            currentBuffer = buffers.pop();
            currentBuffer.flip();
        }
    }
}