package org.neo4j.driver.internal.connector.select;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.spi.Logger;
import org.neo4j.driver.v1.exceptions.ClientException;

/**
 * The Selector-based connector uses a configurable number of background threads to handle incoming network packets.
 * Because many sessions will be idle a lot of the time, and because we'd like to minimize the number of threads, each IO thread is responsible
 * for a set of sessions, rather than having one thread per session.
 */
public class SelectorPool implements AutoCloseable
{
    public static final String THREAD_BASE_NAME = "neo4j-io-";

    private static final AtomicInteger threadIdGen = new AtomicInteger( 0 );
    private final ExecutorService ioThreads;
    private final Selector[] selectors;
    private volatile int selectorSequence = 0;
    private volatile boolean running = true;

    public SelectorPool( int numberOfThreads, final Logger logger )
    {
        try
        {
            ioThreads = Executors.newFixedThreadPool( numberOfThreads, new ThreadFactory()
            {
                @Override
                public Thread newThread( Runnable r )
                {
                    Thread thread = new Thread( r, THREAD_BASE_NAME + threadIdGen.incrementAndGet() );
                    thread.setDaemon( true );
                    return thread;
                }
            } );

            selectors = new Selector[numberOfThreads];

            for ( int i = 0; i < numberOfThreads; i++ )
            {
                final Selector selector = selectors[i] = Selector.open();
                ioThreads.submit( new SelectorWorker( selector, logger ) );
            }
        }
        catch ( IOException e )
        {
            throw new ClientException( "Unable to open selector: " + e.getMessage(), e );
        }
    }

    public synchronized Selector assignSelector()
    {
        return selectors[ (selectorSequence++) % selectors.length];
    }

    @Override
    public void close() throws Exception
    {
        running = false;
        ioThreads.shutdown();
        ioThreads.awaitTermination( 10, TimeUnit.SECONDS );
    }

    private class SelectorWorker implements Runnable
    {
        private final Selector selector;
        private final Logger logger;

        public SelectorWorker( Selector selector, Logger logger )
        {
            this.selector = selector;
            this.logger = logger;
        }

        @Override
        public void run()
        {
            try
            {
                while(running)
                {
                    // Select loop - we block here for up to some timeout waiting for a network event
                    // The reason we don't block indefinitely is to check the `running` flag
                    int numKeys = selector.select( 10 );
                    if ( numKeys > 0 )
                    {
                        // If there were network events, forward each one to the SelectorCallback registered for the channel the event occurred on
                        Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

                        while(keys.hasNext())
                        {
                            SelectionKey key = keys.next();
                            if( key.isReadable() )
                            {
                                SelectorCallback cb = (SelectorCallback) key.attachment();
                                cb.onIncomingNetworkData( (ReadableByteChannel) key.channel() );
                            }

                            keys.remove();
                        }
                    }
                }
            }
            catch( Throwable e )
            {
                logger.error( "An error occurred when processing a network event. The driver will not be able to continue until the network " +
                              "error is resolved. The error message was: " + e.getMessage(), e );
            }
        }
    }
}
