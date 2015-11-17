package org.neo4j.driver.internal.connector.select;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.driver.internal.connector.socket.ChunkedOutput;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.util.BytePrinter;
import org.neo4j.driver.internal.util.ThrowingConsumer;
import org.neo4j.driver.util.RecordingByteChannel;

import static junit.framework.TestCase.assertEquals;

public class BoltV1IncrementalDechunkerTest
{
    @Test
    public void shouldHandleDechunkingMessageCutAtAnySpot() throws Throwable
    {
        // Given
        byte[] chunked = chunkedData();

        // When
        for ( int i = 1; i < chunked.length; i++ )
        {
            ReadableByteChannel part1 = new ByteViewChannel( chunked, 0, i );
            ReadableByteChannel part2 = new ByteViewChannel( chunked, i, chunked.length - i );

            String dechunked = dechunk( part1, part2 );

            // Then
            assertEquals("00 00 00 01 00 00 00 00    00 00 00 02 03 00 00 00    04 00 00 00 00 00 00 00    05 ", dechunked);
        }
    }

    private String dechunk( ReadableByteChannel part1, ReadableByteChannel part2 ) throws IOException
    {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BoltV1IncrementalDechunker dechunker = new BoltV1IncrementalDechunker( new ThrowingConsumer<PackInput, IOException>()
        {
            @Override
            public void accept( PackInput packInput ) throws IOException
            {
                while(packInput.hasMoreData())
                {
                    baos.write( packInput.readByte() );
                }
            }
        });
        dechunker.handle( part1 );
        dechunker.handle( part2 );

        return BytePrinter.hex( baos.toByteArray() );
    }

    private byte[] chunkedData() throws IOException
    {
        RecordingByteChannel ch = new RecordingByteChannel();
        ChunkedOutput chunker = new ChunkedOutput( ch );
        chunker.writeInt( 1 );
        chunker.writeLong( 2 );
        chunker.flush();
        chunker.writeByte( (byte) 3 );
        chunker.writeInt( 4 );
        chunker.writeLong( 5 );
        chunker.writeMessageBoundary();
        chunker.flush();
        return ch.getBytes();
    }

    private static class ByteViewChannel implements ReadableByteChannel
    {
        private final byte[] underlyingData;
        private int readIndex;
        private int channelRemaining;

        public ByteViewChannel( byte[] underlyingData, int offset, int length )
        {
            this.underlyingData = underlyingData;
            this.readIndex = offset;
            this.channelRemaining = length;
        }

        @Override
        public int read( ByteBuffer dst ) throws IOException
        {
            if( channelRemaining > 0 )
            {
                int toCopy = Math.min( dst.remaining(), channelRemaining );
                dst.put( underlyingData, readIndex, toCopy );
                readIndex += toCopy;
                channelRemaining -= toCopy;
                return toCopy;
            }
            return 0;
        }

        @Override
        public boolean isOpen()
        {
            return true;
        }

        @Override
        public void close() throws IOException
        {

        }
    }
}