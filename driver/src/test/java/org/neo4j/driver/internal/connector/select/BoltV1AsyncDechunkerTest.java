package org.neo4j.driver.internal.connector.select;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.internal.connector.socket.ChunkedOutput;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.util.BytePrinter;
import org.neo4j.driver.internal.util.ThrowingConsumer;
import org.neo4j.driver.util.RecordingByteChannel;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class BoltV1AsyncDechunkerTest
{
    @Test
    public void shouldHandleDechunkingMessageCutAtAnySpot() throws Throwable
    {
        // Given
        byte[] chunked = mixedChunkedData();

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

    @Test
    public void shouldHandleLargeDechunkingMessageCutAtAnySpot() throws Throwable
    {
        // Given
        byte[] chunked = chunkedData( 25 ); // 25 bytes, we set dechunking buffer size to 8 below, so this will overflow

        // When
        for ( int i = 1; i < chunked.length; i++ )
        {
            ReadableByteChannel part1 = new ByteViewChannel( chunked, 0, i );
            ReadableByteChannel part2 = new ByteViewChannel( chunked, i, chunked.length - i );

            String dechunked = dechunk( 8, 4, part1, part2 );

            // Then
            assertEquals("00 01 02 03 04 05 06 07    08 09 0a 0b 0c 0d 0e 0f    10 11 12 13 14 15 16 17    18 ", dechunked);
        }
    }

    @Test
    public void allReadOpsShouldWorkAcrossDechunkBufferOverflowBoundaries() throws Throwable
    {
        // Given
        byte[] chunked = chunkedData( 16 );

        // Then reading the dechunked values when they overflow the dechunking buffer should yield the same result as when they do not
        assertThat( dechunking( chunked, asShorts, 1 ), equalTo( dechunking( chunked, asShorts, 24 ) ) );

        // And that result should be correct.
        assertThat( dechunking( chunked, asShorts, 1 ), equalTo( asList( 1, 515, 1029, 1543, 2057, 2571, 3085, 3599 ) ) );

        // Repeat for other types
        assertThat( dechunking( chunked, asIntegers, 1 ), equalTo( dechunking( chunked, asIntegers, 24 ) ) );
        assertThat( dechunking( chunked, asIntegers, 1 ), equalTo( asList( 66051, 67438087, 134810123, 202182159 ) ) );

        assertThat( dechunking( chunked, asLongs, 1 ), equalTo( dechunking( chunked, asLongs, 24 ) ) );
        assertThat( dechunking( chunked, asLongs, 1 ), equalTo( asList( 283686952306183l, 579005069656919567l ) ) );

        assertThat( dechunking( chunked, asDoubles, 1 ), equalTo( dechunking( chunked, asDoubles, 24 ) ) );
        assertThat( dechunking( chunked, asDoubles, 1 ), equalTo( asList( 1.40159977307889E-309d, 5.924543410270741E-270d ) ) );

        assertThat( dechunking( chunked, asFourByteBinaryBlobs, 1 ), equalTo( dechunking( chunked, asFourByteBinaryBlobs, 24 ) ) );
        assertThat( dechunking( chunked, asFourByteBinaryBlobs, 1 ), equalTo( asList( "00010203", "04050607", "08090a0b", "0c0d0e0f") ) );
    }

    public static <T> List<T> dechunking( byte[] chunked, final IOFunction<PackInput, T> reader, int mainBufferSize ) throws IOException
    {
        final List<T> readValues = new ArrayList<>();
        BoltV1AsyncDechunker dechunker = new BoltV1AsyncDechunker(new ThrowingConsumer<PackInput,IOException>()
        {
            @Override
            public void accept( PackInput packInput ) throws IOException
            {
                while(packInput.hasMoreData())
                {
                    readValues.add( reader.apply( packInput ) );
                }
            }
        }, mainBufferSize, 1 );
        dechunker.handle( new ByteViewChannel( chunked, 0, chunked.length ) );
        return readValues;
    }

    private String dechunk( int mainBufferSize, int expandBufferSize, ReadableByteChannel ... parts ) throws IOException
    {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BoltV1AsyncDechunker dechunker = new BoltV1AsyncDechunker( new ThrowingConsumer<PackInput, IOException>()
        {
            @Override
            public void accept( PackInput packInput ) throws IOException
            {
                while(packInput.hasMoreData())
                {
                    baos.write( packInput.readByte() );
                }
            }
        }, mainBufferSize, expandBufferSize );
        for ( ReadableByteChannel part : parts )
        {
            dechunker.handle( part );
        }

        return BytePrinter.hex( baos.toByteArray() );
    }

    private String dechunk( ReadableByteChannel ... parts ) throws IOException
    {
        return dechunk( 1024, 1024, parts );
    }

    private byte[] mixedChunkedData() throws IOException
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

    private byte[] chunkedData( int dechunkedSize ) throws IOException
    {
        RecordingByteChannel ch = new RecordingByteChannel();
        ChunkedOutput chunker = new ChunkedOutput( ch );

        // each iteration adds
        for ( int i = 0; i < dechunkedSize; i++ )
        {
            chunker.writeByte( (byte) (i % 128) );
        }
        chunker.writeMessageBoundary();
        chunker.flush();
        return ch.getBytes();
    }

    private IOFunction<PackInput,Integer> asIntegers = new IOFunction<PackInput,Integer>()
    {
        @Override
        public Integer apply( PackInput packInput ) throws IOException
        {
            return packInput.readInt();
        }
    };

    private IOFunction<PackInput,Integer> asShorts = new IOFunction<PackInput,Integer>()
    {
        @Override
        public Integer apply( PackInput packInput ) throws IOException
        {
            return (int)packInput.readShort();
        }
    };

    private IOFunction<PackInput,Long> asLongs = new IOFunction<PackInput,Long>()
    {
        @Override
        public Long apply( PackInput packInput ) throws IOException
        {
            return packInput.readLong();
        }
    };

    private IOFunction<PackInput,Double> asDoubles = new IOFunction<PackInput,Double>()
    {
        @Override
        public Double apply( PackInput packInput ) throws IOException
        {
            return packInput.readDouble();
        }
    };

    private IOFunction<PackInput,String> asFourByteBinaryBlobs = new IOFunction<PackInput,String>()
    {
        @Override
        public String apply( PackInput packInput ) throws IOException
        {
            byte[] target = new byte[4];
            packInput.readBytes( target, 0, target.length );
            return BytePrinter.hex( target ).replace( " ", "" ).trim();
        }
    };

    interface IOFunction<A,R>
    {
        R apply( A argument ) throws IOException;
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