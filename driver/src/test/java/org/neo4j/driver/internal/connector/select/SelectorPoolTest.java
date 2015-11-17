package org.neo4j.driver.internal.connector.select;

import org.junit.After;
import org.junit.Test;

import java.nio.channels.Selector;

import org.neo4j.driver.internal.logging.DevNullLogger;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotEquals;

public class SelectorPoolTest
{
    SelectorPool toCleanup = null;

    @Test
    public void shouldProvideRoundRobinSelectors() throws Throwable
    {
        // Given a pool with two selector threads
        SelectorPool pool = toCleanup = new SelectorPool( 2, new DevNullLogger() );

        // When
        Selector s1 = pool.assignSelector();
        Selector s2 = pool.assignSelector();
        Selector s3 = pool.assignSelector();
        Selector s4 = pool.assignSelector();
        Selector s5 = pool.assignSelector();
        Selector s6 = pool.assignSelector();

        // Then odd ones should be the same selector (eg. assigned to the same thread)
        assertEquals( s1, s3 );
        assertEquals( s1, s5 );

        // And even ones should also be the same
        assertEquals( s2, s4 );
        assertEquals( s2, s6 );

        // And odd/even should not be the same
        assertNotEquals( s1, s2 );
        assertNotEquals( s1, s4 );
        assertNotEquals( s1, s6 );
    }

    @Test
    public void shouldShutdownThreads() throws Throwable
    {
        // Given a pool with two selector threads
        SelectorPool pool = new SelectorPool( 2, new DevNullLogger() );

        // When
        pool.close();

        // Then there should not be any selector threads
        assertThat( thereAreSelectorThreads(), is(false));
    }

    @After
    public void cleanup() throws Exception
    {
        if( toCleanup != null )
        {
            toCleanup.close();
        }
    }

    protected boolean thereAreSelectorThreads()
    {
        for ( Thread thread : Thread.getAllStackTraces().keySet() )
        {
            if(thread.getName().contains( SelectorPool.THREAD_BASE_NAME ))
            {
                return true;
            }
        }
        return false;
    }
}