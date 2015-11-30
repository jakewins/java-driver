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
package org.neo4j.driver.stress;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.connector.select.BoltV1AsyncResultCollector;
import org.neo4j.driver.internal.connector.select.SelectorConnector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.util.Neo4jRunner;

import static org.neo4j.driver.Config.defaultConfig;
import static org.neo4j.driver.Values.parameters;

public class ConnectorStresser
{
    private static Neo4jRunner server;
    private static SelectorConnector connector;

    private static String statement;

    private final static String SINGLE_ROW = "RETURN 1 AS n";
    private final static String THOUSAND_ROW = "UNWIND x in range(1,1000) RETURN 1 AS n";

    public static void main( String... args ) throws Throwable
    {
        int iterations = 200_000;

        statement = SINGLE_ROW;
        bench( iterations, 1, 10_000 );
        bench( (long) iterations / 2, 2, 10_000 );
        bench( (long) iterations / 4, 4, 10_000 );
        bench( (long) iterations / 8, 8, 10_000 );

        statement = THOUSAND_ROW;
        bench( iterations, 1, 10_000 );
        bench( (long) iterations / 2, 2, 10_000 );
        bench( (long) iterations / 4, 4, 10_000 );
        bench( (long) iterations / 8, 8, 10_000 );
    }

    public static void setup() throws Exception
    {
        server = Neo4jRunner.getOrCreateGlobalRunner();
        server.startServerOnEmptyDatabase();
        connector = new SelectorConnector();
    }

    public static void tearDown() throws Exception
    {
        connector.close();
        server.stopServerIfRunning();
    }

    static class Worker
    {
        private final Connection session;

        private final Map<String,Value> parameters = parameters();

        private BoltV1AsyncResultCollector collector = new BoltV1AsyncResultCollector();

        public Worker()
        {
            session = connector.connect( URI.create( "bolt://localhost" ), defaultConfig() );
        }

        public int operation()
        {
            int total = 0;
            session.run( statement, parameters, collector );
            session.pullAll( null );
            session.sync();

            for ( Record record = collector.next(); record != null; record = collector.next() )
            {
                total += record.get( 0 ).javaInteger();
            }

            collector.reset();

            return total;
        }
    }


    private static void bench( long iterations, int concurrency, long warmupIterations ) throws Exception
    {
        ExecutorService executorService = Executors.newFixedThreadPool( concurrency );

        setup();
        try
        {
            // Warmup
            awaitAll( executorService.invokeAll( workers( warmupIterations, concurrency ) ) );

            long start = System.nanoTime();
            List<Future<Object>> futures = executorService.invokeAll( workers( iterations, concurrency ) );
            awaitAll( futures );
            long delta = System.nanoTime() - start;

            System.out.printf( "With %d threads: %s ops/s%n",
                               concurrency, (iterations * concurrency) / (delta / 1_000_000_000.0) );
        }
        finally
        {
            tearDown();
        }

        executorService.shutdownNow();
        executorService.awaitTermination( 10, TimeUnit.SECONDS );
    }

    private static void awaitAll( List<Future<Object>> futures ) throws Exception
    {
        for ( Future<Object> future : futures )
        {
            future.get();
        }
    }

    private static List<Callable<Object>> workers( final long iterations, final int numWorkers )
    {
        List<Callable<Object>> workers = new ArrayList<>();
        for ( int i = 0; i < numWorkers; i++ )
        {
            final Worker worker = new Worker();
            workers.add( new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    int dontRemoveMyCode = 0;
                    for ( int i = 0; i < iterations; i++ )
                    {
                        dontRemoveMyCode += worker.operation();
                    }
                    return dontRemoveMyCode;
                }
            } );
        }
        return workers;
    }
}
