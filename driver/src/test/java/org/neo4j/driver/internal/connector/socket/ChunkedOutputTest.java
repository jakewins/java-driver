/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.connector.socket;

import org.hamcrest.MatcherAssert;
import org.junit.Test;

import org.neo4j.driver.internal.util.BytePrinter;
import org.neo4j.driver.v1.util.RecordingByteChannel;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ChunkedOutputTest
{
    private final RecordingByteChannel channel = new RecordingByteChannel();
    private final ChunkedOutput out = new ChunkedOutput( 16, channel );

    @Test
    public void shouldChunkSingleMessage() throws Throwable
    {
        // When
        out.writeByte( (byte) 1 ).writeShort( (short) 2 );
        out.writeMessageBoundary();
        out.flush();

        // Then
        MatcherAssert.assertThat( BytePrinter.hex( channel.getBytes() ),
                equalTo( "00 03 01 00 02 00 00 " ) );
    }

    @Test
    public void shouldChunkMessageSpanningMultipleChunks() throws Throwable
    {
        // When
        out.writeLong( 1 )
           .writeLong( 2 )
           .writeLong( 3 );
        out.writeMessageBoundary();
        out.flush();

        // Then
        assertThat( BytePrinter.hex( channel.getBytes() ), equalTo( String.format(
                "00 08 00 00 00 00 00 00    00 01 00 08 00 00 00 00    " +
                "00 00 00 02 00 08 00 00    00 00 00 00 00 03 00 00%n" ) ) );
    }

    @Test
    public void shouldReserveSpaceForChunkHeaderWhenWriteDataToNewChunk() throws Throwable
    {
        // Given 2 bytes left in buffer + chunk is closed
        out.writeBytes( new byte[10], 0, 10 );  // 2 (header) + 10
        out.writeMessageBoundary();             // 2 (ending)

        // When write 2 bytes
        out.writeShort( (short) 33 );           // 2 (header) + 2

        // Then the buffer should auto flash if space left (2) is smaller than new data and chunk header (2 + 2)
        assertThat( BytePrinter.hex( channel.getBytes() ),
                equalTo( "00 0a 00 00 00 00 00 00    00 00 00 00 00 00 " ) );
    }

    @Test
    public void shouldSendOutDataWhoseSizeIsGreaterThanOutputBufferCapacity() throws Throwable
    {
        out.writeBytes( new byte[16], 0, 16 );  // 2 + 16 is greater than the default max size 16
        out.writeMessageBoundary();
        out.flush();

        assertThat( BytePrinter.hex( channel.getBytes() ),
                equalTo( "00 0e 00 00 00 00 00 00    00 00 00 00 00 00 00 00    00 02 00 00 00 00 " ) );
    }
}
