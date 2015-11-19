package org.neo4j.driver.internal.connector.select;

import java.util.List;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

import org.neo4j.driver.Notification;
import org.neo4j.driver.Plan;
import org.neo4j.driver.ProfiledPlan;
import org.neo4j.driver.StatementType;
import org.neo4j.driver.UpdateStatistics;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.spi.StreamCollector;

public class BoltV1AsyncResultCollector implements StreamCollector
{
    private final TransferQueue<Value[]> recievedRecords = new LinkedTransferQueue<>();

    /** Read the next value, or block until it arrives */
    public Value[] next()
    {
        try
        {
            // TODO batch ops
            return recievedRecords.take();
        }
        catch ( InterruptedException e )
        {
            throw new ClientException( "Interrupted while waiting for the next record to arrive in a stream.", e );
        }
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
    public void fieldNames( String[] names )
    {
        // TODO
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
