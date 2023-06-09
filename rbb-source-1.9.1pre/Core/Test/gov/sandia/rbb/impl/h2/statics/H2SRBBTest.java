/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.impl.h2.statics;

import java.sql.Connection;
import java.sql.ResultSet;
import gov.sandia.rbb.*;
import java.sql.SQLException;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author klakkar
 */
public class H2SRBBTest {

    private static RBB findSeqsRBB;

    @Test
    public void dummy() {
        // there is no test in this file, it's just utilities.
    }

    public H2SRBBTest()
            throws Exception
    {
        gov.sandia.rbb.impl.h2.statics.H2SRBBTest.checkAssertEnabled();
    }

    /**
     * Throw an exception unless assert is enabled.
     * All tests that rely on asserts to test for correctness should call this, e.g in the constructor:
     * gov.sandia.rbb.impl.h2.statics.H2SRBBTest.checkAssertEnabled();
     * In RBB, tests that rely on assert to do checking, also rely on asserted statements for side effects sometimes.
     * There is no harm since runnign without asserts inherently breaks the code by preventing it from checking correctness.
     * 
     */
    public static void checkAssertEnabled()
        throws Exception
    {
        boolean assertsEnabled = false;
        assert assertsEnabled = true;  // Intentional side-effect!!!
        if (assertsEnabled == false)
        {
            throw new Exception(
                "Error - assert must be enabled for test classes, otherwise they don't actually do checking!  Use Java VM option -enableassertions");
        }
    }


    /**
     *  utility function to turn a result set into a string
     * @param rs
     * @return
     * @throws java.sql.SQLException
     */
    public static String toString(ResultSet rs) throws java.sql.SQLException {
        String s = "";

        for(int i = 0 ; i < rs.getMetaData().getColumnCount(); ++i) {
            if(i>0) { s += "\t"; }
            s += rs.getMetaData().getColumnName(i+1);
        }
        s += "\n";

        while(rs.next()) {
            for(int i = 0 ; i < rs.getMetaData().getColumnCount(); ++i) {
                if(i>0) { s += "\t";}
                s += rs.getString(i+1);
            }
            s += "\n";
        }
        return s;
    }

    /**
     * utility function to run a query and print the restults to stderr.
     * @param conn
     * @param q
     * @throws java.sql.SQLException
     */
    public static String queryResultsToString(Connection conn, String q) throws java.sql.SQLException {
        String result = toString(conn.createStatement().executeQuery(q));
        // System.err.println(result);
        return result;
    }

    /**
     * utility function to run a query and print the restults to stderr.
     * @param conn
     * @param q
     * @throws java.sql.SQLException
     */
    public static void printQueryResults(Connection conn, String q) throws java.sql.SQLException {
        System.err.println(queryResultsToString(conn, q));
    }

    /**
     * utility function to test remaining rows of results in a query.
     * @param rs
     * @return
     * @throws java.sql.SQLException
     */
    public static int countRemainingRows(ResultSet rs) throws java.sql.SQLException {
        int i = 0;
        while(rs.next()) {
            ++i;
        }
        return i;
    }

    /**
     * Run the specified query and return count of results.
     * @param conn
     * @param q
     * @return
     * @throws java.sql.SQLException
     */
    static int countQueryResults(Connection conn, String q) throws java.sql.SQLException {
        ResultSet rs = conn.createStatement().executeQuery(q);
        return countRemainingRows(rs);
    }

    
}