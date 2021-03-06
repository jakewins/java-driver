/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal.connector.socket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat.Reader;
import org.neo4j.driver.internal.messaging.MessageFormat.Writer;
import org.neo4j.driver.internal.spi.Logger;

import static org.neo4j.driver.internal.connector.socket.ProtocolChooser.bytes2Int;
import static org.neo4j.driver.internal.connector.socket.ProtocolChooser.chooseVersion;
import static org.neo4j.driver.internal.connector.socket.ProtocolChooser.supportedVersions;

public class SocketClient
{
    private final String host;
    private final int port;

    private Socket socket;

    private OutputStream out;
    private InputStream in;

    private SocketProtocol protocol;
    private Reader reader;
    private Writer writer;

    public SocketClient( String host, int port, Logger logger )
    {
        this.host = host;
        this.port = port;
    }

    public void start()
    {
        try
        {
            socket = new Socket();
            socket.connect( new InetSocketAddress( host, port ) );
            out = socket.getOutputStream();
            in = socket.getInputStream();

            protocol = negotiateProtocol();
            protocol.outputStream( out );
            protocol.inputStream( in );
            reader = protocol.reader();
            writer = protocol.writer();
        }
        catch ( ConnectException e )
        {
            throw new ClientException( String.format( "Unable to connect to '%s' on port %s, " +
                                                      "ensure the database is running and that there is a working " +
                                                      "network " +
                                                      "connection to it.", host, port ) );
        }
        catch ( IOException e )
        {
            throw new ClientException( "Unable to process request: " + e.getMessage(), e );
        }
    }

    public void send( List<Message> pendingMessages, SocketResponseHandler handler ) throws IOException
    {
        for ( Message message : pendingMessages )
        {
            writer.write( message );
        }
        writer.flush();

        // well, we block and wait for the response.
        while ( handler.countsOfResponses() < pendingMessages.size() )
        {
            reader.read( handler );
        }
    }

    public void stop()
    {
        try
        {
            socket.close();
        }
        catch ( IOException e )
        {
            throw new ClientException( "Unable to close socket connection properly." + e.getMessage(), e );
        }
    }

    Writer writer()
    {
        return this.writer;
    }

    Reader reader()
    {
        return this.reader;
    }

    private SocketProtocol negotiateProtocol() throws IOException
    {
        out.write( supportedVersions() );
        byte[] accepted = new byte[4];
        in.read( accepted );
        int version = bytes2Int( accepted );
        return chooseVersion( version );
    }

    @Override
    public String toString()
    {
        return "" + protocol.version();
    }
}
