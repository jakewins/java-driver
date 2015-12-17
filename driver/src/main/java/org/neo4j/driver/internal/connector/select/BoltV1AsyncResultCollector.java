package org.neo4j.driver.internal.connector.select;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.spi.StreamCollector;
import org.neo4j.driver.v1.Notification;
import org.neo4j.driver.v1.Plan;
import org.neo4j.driver.v1.ProfiledPlan;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementType;
import org.neo4j.driver.v1.UpdateStatistics;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.Arrays.asList;

public class BoltV1AsyncResultCollector implements StreamCollector
{
    private static final Value[] EOF = new Value[0];

    private final TransferQueue<Value[]> recievedRecords = new LinkedTransferQueue<>();

    private Map<String, Integer> fieldLookup;
    private List<String> fieldOrder;
    private boolean eof = false;

    /** Read the next value, or block until it arrives */
    public Record next()
    {
        try
        {
            if( eof )
            {
                return null;
            }

            // TODO batch ops
            Value[] record = recievedRecords.take();
            if( record != EOF )
            {
                return new InternalRecord( fieldOrder, fieldLookup, record );
            }
            else
            {
                eof = true;
                return null;
            }
        }
        catch ( InterruptedException e )
        {
            throw new ClientException( "Interrupted while waiting for the next record to arrive in a stream.", e );
        }
    }

    public BoltV1AsyncResultCollector reset()
    {
        eof = false;
        fieldLookup = null;
        recievedRecords.clear();
        return this;
    }

    @Override
    public void record( Value[] fields )
    {
        try
        {
            recievedRecords.put( fields );
        }
        catch ( InterruptedException e )
        {
            throw new ClientException( "Interrupted while transferring record from network to application.", e );
        }
    }

    @Override
    public void tail()
    {
        try
        {
            recievedRecords.put( EOF );
        }
        catch ( InterruptedException e )
        {
            throw new ClientException( "Interrupted while transferring record from network to application.", e );
        }
    }

    @Override
    public void head( String[] names )
    {
        fieldOrder = asList(names);
        fieldLookup = new HashMap<>();
        for ( int i = 0; i < names.length; i++ )
        {
            fieldLookup.put( names[i], i );
        }
    }

    @Override
    public void statementType( StatementType type )
    {
        // TODO
    }

    @Override
    public void statementStatistics( UpdateStatistics statistics )
    {
        // TODO
    }

    @Override
    public void plan( Plan plan )
    {
        // TODO
    }

    @Override
    public void profile( ProfiledPlan plan )
    {
        // TODO
    }

    @Override
    public void notifications( List<Notification> notifications )
    {
        // TODO
    }
}
