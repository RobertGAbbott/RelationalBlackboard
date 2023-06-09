/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.examples;

import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.RBB;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author rgabbot
 */
public class RBBTutorial
{
    public static void main(String[] args)
    {
        try
        {
            ////// Create an RBB
            // This creates the H2 database, and adds RBB functionality to it.
            RBB rbb = RBB.create("jdbc:h2:mem:HelloWorldDB", null);

            ////// Creating a Sequence
            Timeseries ts = new Timeseries(rbb, 1, 0, new Tagset("test=tutorial,n=1"));

            ////// Adding an Observation to a Sequence
            ts.add(rbb, 2.0, 22.2f);
            ts.add(rbb, 3.0, 33.3f);

            ////// Ending a Sequence
            ts.setEnd(rbb.db(), 4.0);

            ////// Finding Sequences
            Timeseries[] a = Timeseries.findWithSamples(rbb.db(), byTags("n=1"));
            System.err.println("I found " + a.length + " timeseries");

            ////// Retrieving Observations
            Float[] x = a[0].value(3.5);
            System.err.println("My interpolated value at time 3.5 is " + x[0]);

            ////// Deleting
            int n = H2SEvent.delete(rbb.db(), byTags("test=tutorial"));
            System.err.println("I deleted " + n + " sequences");
        }
        catch (java.sql.SQLException e)
        {
            System.err.println("error: " + e.getLocalizedMessage());
        }

    }
}
