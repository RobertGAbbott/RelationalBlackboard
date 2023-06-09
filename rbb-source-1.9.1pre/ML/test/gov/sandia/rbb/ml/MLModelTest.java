/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml;

import gov.sandia.rbb.ml.features.DistanceFE;
import gov.sandia.rbb.RBB;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rgabbot
 */
public class MLModelTest {

    public static class TestModel extends MLModel {

        @Override
        public MLFeatureExtractor getPredictionFE() throws Exception {
            return getTrainingFE();
        }

        @Override
        public MLFeatureExtractor getTrainingFE() throws Exception {
            return new DistanceFE("x", "a", "_b", DistanceFE.DistanceType.SCALAR,
                    new DistanceFE("_y", "a", "_b", DistanceFE.DistanceType.SCALAR, null));
        }

        @Override
        public String[] getInputNames(RBBML.Mode m) {
            return new String[] { "a", "_b" }; // y starts with underscore so not stored in DB (or used as predictor) by default.
        }
    };

    @Test
    public void testModelInitialization() throws Exception {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        String jdbc = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(jdbc, null);

        MLModel model = MLModel.create(rbb, TestModel.class.getName(), "");

        assertEquals("x", model.getPredictionName()); // last input or derived feature that doesn't start with underscore.
        assertArrayEquals(new String[]{"a", "x"}, model.getDBFeatureNames());
        assertArrayEquals(new String[]{"a"}, model.getPredictorNames());

        // assertEquals(model.getDBFeatureNames());

        rbb.disconnect();
    }

}
