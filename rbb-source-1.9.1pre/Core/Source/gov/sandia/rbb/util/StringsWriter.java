/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.util;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Here is a utility class that is handy for creating a string
 * representation of a SQL query (or any array of String)
 */
public class StringsWriter extends StringWriter {
    public void writeStrings(String... a) { for(String a0 : a) { write(a0); } };

    /**
     * Note, changing the second arg to Object... causes:<p>
     * warning: non-varargs call of varargs method with inexact argument type for last parameter;<p>
     * In several places.
     */
    public void writeJoin(String d, Object[] a) {
        if(a==null) {
            write("NULL");
            return;
        }
        for(int i=0; i < a.length; ++i) {
            if(i>0)
                write(d);
            if(a[i]==null)
                write("null");
            else
                write(a[i].toString());
        }
    }

    public static String join(String d, Object[] a) {
        StringsWriter sw = new StringsWriter();
        sw.writeJoin(d,a);
        return sw.toString();
    };

    public void writeObjects(Object[] a) {
        write("(");
        for(int i=0; i < a.length; ++i) {
            if(i>0) write(",");
            if(a[i] == null)
                write("null");
            else if(a[i] instanceof Object[])
                writeObjects((Object[])a[i]);
            else
                write(a[i].toString());
        }
        write(")");
    }

    public void writeQueryResults(ResultSet rs) throws SQLException {
        for(int i=0; i < rs.getMetaData().getColumnCount(); ++i) {
            write("\t");
            write(rs.getMetaData().getColumnName(i+1));
        }
        write("\n");
        while(rs.next()) {
            for(int i = 0; i < rs.getMetaData().getColumnCount(); ++i) {
                write("\t");
                if(rs.getObject(i+1)==null)
                    write("null");
                else if(rs.getObject(i+1) instanceof java.sql.Array)
                    writeObjects((Object[]) rs.getArray(i+1).getArray());
                else if(rs.getObject(i + 1) instanceof Object[])
                    writeObjects((Object[]) rs.getObject(i+1));
                else
                    write(rs.getObject(i+1).toString());
            }
            write("\n");
        }
    }
    public void writeQueryResults(Connection conn, String query) throws SQLException {
        writeQueryResults(conn.createStatement().executeQuery(query));
    }

    /**
     * Erase contents and start over.
     */
    public void empty() {
        getBuffer().setLength(0);
    }
}
