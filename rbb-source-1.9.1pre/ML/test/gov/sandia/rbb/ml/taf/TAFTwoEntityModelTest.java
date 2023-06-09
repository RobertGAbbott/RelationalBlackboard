/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.taf;

import gov.sandia.rbb.ml.MLModel;
import gov.sandia.rbb.tools.RBBSelection;
import gov.sandia.rbb.ml.models.TAFTwoEntityModel;
import static gov.sandia.rbb.ml.RBBML.RBBMLMain;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.RBB;
import org.junit.Test;
import static org.junit.Assert.*;
import static gov.sandia.rbb.Tagset.TC;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author rgabbot
 */
public class TAFTwoEntityModelTest {
    @Test
    public void testMLSession() throws Exception {
        testTAFTwoEntityModel("jdbc:h2:mem:");
    }

    /**
     * If run from the command-line, this test will create a persistent database
     * (instead of in-memory as used for the actual unit test).
     * This way the database can be inspected if the test is failing.
     *
     * S=jdbc:h2:tcp:localhost//tmp/testSession; M=jdbc:h2:tcp:localhost//tmp/testModel
     * rm /tmp/testSession.* /tmp/testModel.*
     * $RUNML draw -filterTags color -server $S
     */
    public static void main(String[] args) {
        try {
            testTAFTwoEntityModel("jdbc:h2:file:/tmp/test");
        } catch (Exception ex) {
            System.err.println(ex.toString());
            System.exit(-1);
        }
    }

    private static void testTAFTwoEntityModel(String where) throws Exception {

        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        //// 1. create a session ////
        final String username = "John Doe";
        final String sessionURL = where + "Session";
        RBB session = RBB.create(sessionURL, "Session");
        // the leader goes along the x axis at 1 unit per second from t=0 to 5, then goes up x=5 at the same speed.
        Timeseries leader = new Timeseries(session, 2, 0.0, TC("color=red,description=leader"));
        RBBSelection selection = RBBSelection.oneShot(session.db(), session.db());
        selection.selectEvents(byID(leader.getID()));
        leader.add(session, 0.0, 0.0f,0.0f);
        leader.add(session, 5.0, 5.0f,0.0f);
        leader.add(session, 10.0, 5.0f,5.0f);
        leader.setEnd(session.db(), 10.0);
        // now the example follower, which is the same thing with a 0.5 s delay.
        Timeseries follower = new Timeseries(session, 2, 0.5, TC("color=green,description=follower"));
        selection.selectEvents(byID(follower.getID()));
        follower.add(session, 0.5, 0.0f,0.0f);
        follower.add(session, 5.5, 5.0f,0.0f);
        follower.add(session, 10.5, 5.0f,5.0f);
        follower.setEnd(session.db(), 10.5);
        //// session variable must be kept, or the session will disappear since it's memory-based.
        // session.disconnect();

        //// 2. create a model ////

        final String modelURL = where + "Model";
        // must create the model as a variable also so it doesn't disappear even if it is memory-based.
        // MLMain(new String[]{"create", modelURL, "gov.sandia.rbb.ml.models.TwoEntityModel", "mymodel"});
        MLModel model = MLModel.create(RBB.create(modelURL, null), TAFTwoEntityModel.class.getCanonicalName(), username);

        //// 3. add training examples ////
        RBBMLMain("setSimTime", "-rbb", sessionURL, "2", "4");
        RBBMLMain("train", "-rbb", sessionURL, "-modelRBB", modelURL, "1");
        // MLMain(new String[]{"print", sessionURL, modelURL});
        
        //// 4. generate a behavior (stored back into the session) ////
        RBBMLMain("predict", "-rbb", sessionURL, "-modelRBB", modelURL, "-inputs", "color=red", "-0.5", "0");
        //// 5. see whether the generated behavior is where expected at various times. ////
        Timeseries[] tsa = Timeseries.findWithSamples(session.db(), byTags(model.getMinimalResultTags()));
        assertEquals(1, tsa.length);

        Timeseries taf = tsa[0];
        // the TAF entity should be right over the example entity, both before and somewhat after the leader makes a turn.
        assertEqualArray(follower.value(3.0), taf.value(3.0), 1e-6f);
        assertEqualArray(follower.value(7.0), taf.value(7.0), 1e-6f);

        session.disconnect();
    }

    /**
     * Make sure the two timeseries have equal values at the specified time.
     */
    private static void assertEqualArray(Float[] a, Float[] b, float d) {
        assertEquals(a.length, b.length);
        for(int i = 0; i < a.length; ++i)
            assertEquals(a[i], b[i], d);
    }
}