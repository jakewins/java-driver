package org.neo4j.driver.internal.connector.select;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;

import org.neo4j.driver.internal.messaging.MessageHandler;
import org.neo4j.driver.internal.spi.StreamCollector;
import org.neo4j.driver.v1.Value;

import static org.neo4j.driver.v1.Values.valueToString;

/**
 * Handles routing Bolt V1 streams to registered listeners.
 */
public class BoltV1MessageRouter extends MessageHandler.Adapter
{
    // TODO this is not thread safe, and this field will be accessed by both the app thread and the IO thread
    private final LinkedTransferQueue<StreamCollector> queuedObservers = new LinkedTransferQueue<>();
    private StreamCollector currentObserver;

    enum State
    {
        /** Before the session can be used, we send an initialization message, the reply from that is handled here. */
        INIT
        {
            @Override
            State success( BoltV1MessageRouter ctx, Map<String,Value> meta )
            {
                // Init completed
                return IDLE;
            }
        },

        /** This state means there is not currently a stream open. When we recieve a `SUCCESS` message here, it will always be the beginning of a stream */
        IDLE
        {
            @Override
            State success( BoltV1MessageRouter ctx, Map<String,Value> meta )
            {
                // Success message when idle means new stream!
                ctx.currentObserver = ctx.queuedObservers.poll();

                // Read HEAD metadata
                List<String> fields = meta.get( "fields" ).asList( valueToString() );
                ctx.currentObserver.head( fields.toArray( new String[fields.size()] ) );
                return IN_STREAM;
            }
        },

        /** This state means we are currently processing records in a stream. */
        IN_STREAM
        {
            @Override
            State record( BoltV1MessageRouter ctx, Value[] fields )
            {
                ctx.currentObserver.record( fields );
                return IN_STREAM;
            }

            @Override
            State success( BoltV1MessageRouter ctx, Map<String,Value> meta )
            {
                ctx.currentObserver.tail();
                return IDLE;
            }
        };

        // TODO - implement defaults for all methods below, left them like this to get benchmarks to validate this direction

        State success( BoltV1MessageRouter ctx, Map<String,Value> meta )
        {
            throw new UnsupportedOperationException();
        }

        State record( BoltV1MessageRouter ctx, Value[] fields )
        {
            throw new UnsupportedOperationException();
        }

        State failure( BoltV1MessageRouter ctx, String code, String message )
        {
            throw new UnsupportedOperationException();
        }

        State ignored( BoltV1MessageRouter ctx )
        {
            throw new UnsupportedOperationException();
        }
    }

    private State state = State.INIT;

    /** Queue up a stream observer, once all prior observers have seen their streams, this observer will see the stream that comes next */
    public void register( StreamCollector collector )
    {
        queuedObservers.add( collector );
    }

    @Override
    public void handleSuccessMessage( Map<String,Value> meta ) throws IOException
    {
        state = state.success( this, meta );
    }

    @Override
    public void handleRecordMessage( Value[] fields ) throws IOException
    {
        state = state.record( this, fields );
    }

    @Override
    public void handleFailureMessage( String code, String message ) throws IOException
    {
        state = state.failure( this, code, message );
    }

    @Override
    public void handleIgnoredMessage() throws IOException
    {
        state = state.ignored( this );
    }
}
