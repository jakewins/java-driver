package org.neo4j.driver.internal.connector.select;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.internal.messaging.MessageHandler;
import org.neo4j.driver.internal.spi.StreamCollector;

/**
 * Handles routing Bolt V1 streams to registered listeners.
 */
public class BoltV1MessageRouter extends MessageHandler.Adapter
{
    // TODO this is not thread safe, and this field will be accessed by both the app thread and the IO thread
    private final LinkedList<StreamCollector> queuedObservers = new LinkedList<>();
    private StreamCollector currentObserver;

    enum State
    {
        INIT
        {
            @Override
            State success( BoltV1MessageRouter ctx, Map<String,Value> meta )
            {
                // Init completed
                return IDLE;
            }
        },
        IDLE
        {
            @Override
            State success( BoltV1MessageRouter ctx, Map<String,Value> meta )
            {
                // Success message when idle means new stream!
                ctx.currentObserver = ctx.queuedObservers.pop();

                // Read HEAD metadata
                System.out.println(meta);
//                Value fields = meta.get( "fields" );
//                List<String> fields = fields.javaList( valueToString() );
//                ctx.currentObserver.fieldNames( fields.toArray(new String[fields.size()]) );
                return IN_STREAM;
            }
        },
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

    private State state = State.IDLE;

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
