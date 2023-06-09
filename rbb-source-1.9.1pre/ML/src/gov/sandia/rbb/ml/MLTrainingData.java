/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml;

import gov.sandia.rbb.RBB;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Accessor class for training data that has previously been added to a model (i.e. stored in its RBB)
 *
 */
public interface MLTrainingData {

    public boolean next() throws SQLException;

    public int getNumPredictors() throws SQLException;

    /**
     * Get the data for the ith predictor.  i is 0-based.
     */
    public Object[] getPredictor(int i) throws SQLException;

    public Object[] getResponse() throws SQLException;

    static class FromRBB implements MLTrainingData  {

        private ResultSet rs;

        /**
         *
         * This is only constructed by MLModel.getTrainingData()
         *
         */
        FromRBB(RBB rbb, String[] predictorNames, String predictionName) throws SQLException {
            StringWriter q = new StringWriter();
            q.write("select TRAINING_EVENT, ");
            for(int i = 0; i < predictorNames.length; ++i) {
                q.write("\""); // quoting makes the column name case-sensitive.
                q.write(predictorNames[i]);
                q.write("\",");
            }
            q.write(" \""+predictionName+"\"");
            q.write(" from RBBML_TRAINING_DATA where WEIGHT > 0;");
            rs = rbb.db().createStatement().executeQuery(q.toString());
        }

        public boolean next() throws SQLException {
            return rs.next();
        }

        public int getNumPredictors() throws SQLException {
            return rs.getMetaData().getColumnCount()-2; // -2 because first col is TRAINING_EVENT and last col is response.
        }

        /**
         * Get the data for the ith predictor.  i is 0-based.
         */
        public Object[] getPredictor(int i) throws SQLException {
            return (Object[])rs.getArray(i+2).getArray(); // +2 because getArray is 1-based and first col is TRAINING_EVENT
        }

        public Object[] getResponse() throws SQLException {
            return (Object[]) rs.getArray(rs.getMetaData().getColumnCount()).getArray(); // last col is response.
        }

        public Object[][] getPredictors() throws SQLException {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }


    static class Rewindable implements MLTrainingData {

        private ArrayList<Object[][]> predictors;
        private ArrayList<Object[]> responses;
        Integer iNext = null;
        Integer numPredictors = null;

        Rewindable(MLTrainingData td) throws SQLException {
            predictors = new ArrayList<Object[][]>();
            responses = new ArrayList<Object[]>();
            numPredictors = td.getNumPredictors();
            while(td.next()) {
                responses.add(td.getResponse());
                Object[][] p = new Object[numPredictors][];
                for(int i = 0; i < numPredictors; ++i)
                    p[i] = td.getPredictor(i);
                predictors.add(p);
            }
            iNext=-1;
        }

        public void rewind() {
            iNext=-1;
        }

        public boolean next() throws SQLException {
            if(iNext >= predictors.size()-1)
                return false;
            ++iNext;
            return true;
        }

        public int getNumPredictors() throws SQLException {
            return numPredictors;
        }

        public Object[] getPredictor(int i) throws SQLException {
            return predictors.get(iNext)[i];
        }

        public Object[] getResponse() throws SQLException {
            return responses.get(iNext);
        }
    }
}
