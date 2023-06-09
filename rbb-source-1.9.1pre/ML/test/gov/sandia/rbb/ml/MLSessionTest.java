/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml;

import gov.sandia.rbb.ui.RBBReplayControl;
import gov.sandia.rbb.tools.RBBSelection;
import gov.sandia.rbb.Event;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.RBB;
import static gov.sandia.rbb.Tagset.TC;
import org.junit.Test;
import static org.junit.Assert.*;
import static gov.sandia.rbb.ml.RBBML.RBBMLMain;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author rgabbot
 *
 * To run the GUI on the session this creates:
 * 
 * rm /tmp/test*.db # clear out previous run.
 * cd ~/workspace/Spatial/RelationalBlackboard; . ./environment
 * 
 * S=jdbc:h2:tcp:localhost//tmp/testSession; M=jdbc:h2:tcp:localhost//tmp/testModel
 * 
 * <now run this program>
 *  
 * $RBB ui draw -showpaths -filterTags color -server $S
 * $RBB ml gui $S jdbc:h2:tcp:localhost//tmp/test -applyGrouping type,type -model Model
 * $RBB timeline -multiTimeline type=result,model=Model,color $S
 *
 */
public class MLSessionTest {

    @Test
    public void testMLSession() throws Throwable {
        testMLSession("jdbc:h2:mem:");
    }

    /**
     * If run from the command-line, this test will create a persistent database
     * (instead of in-memory as used for the actual unit test).
     * This way the database can be inspected if the test is failing.
     */
    public static void main(String[] args) {
        try {
            testMLSession("jdbc:h2:file:/tmp/test");
        } catch (Throwable ex) {
            System.err.println(ex.toString());
            System.exit(-1);
        }
    }

    /**
     * 
     * This walks RBBML through most of its functionality
     *<p>
     * It also tests splitting the RBBML into the maximum number of RBBs (4) and
     * using read-only RBBs everywhere possible.
     *<p>
     * Note: this code is intentionally byzantine, since it is testing everything
     * through the command-line interface.  In actual java code, you would
     * create an RBBML instance and then make calls on it instead of calling
     * RBBMLMain.
     *
     */
    private static void testMLSession(String baseURL) throws Throwable {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        final String username = "John Doe";

        // create RBB with 4 timeseries around a circle: gray stationary at top, white stationary at bottom, green counterclockwise, blue clockwise
        final double start=Math.PI/4;
        final double end = start + 4 * Math.PI;
        RBB session = RBB.create(baseURL+"Session;DB_CLOSE_DELAY=-1", null); // infinite close delay so memory db doesn't disappear when closed.
        Timeseries top = new Timeseries(session, 2, start, TC("color=gray,type=fixed,description=top"));
        top.add(session, 0.0, 0.0f, 1.0f);
        top.setEnd(session.db(), end);
        Timeseries bottom = new Timeseries(session, 2, start, TC("color=white,type=fixed,description=bottom"));
        bottom.add(session, 0.0, 0.0f, -1.0f);
        bottom.setEnd(session.db(), end);
        Timeseries sin = new Timeseries(session, 2, start, TC("color=blue,type=dynamic,description=sin"));
        Timeseries cos = new Timeseries(session, 2, start, TC("color=green,type=dynamic,description=cos"));
        final int numSamples=100;
        for(int i = 0; i < numSamples; ++i) {
            double t = start + i * end / (numSamples-1);
            sin.add(session, t, (float)Math.sin(t), (float)Math.cos(t));
            cos.add(session, t, (float)Math.cos(t), (float)Math.sin(t));
        }
        sin.setEnd(session.db(), end);
        cos.setEnd(session.db(), end);

        // from now on, access the session as read-only
        session.disconnect();
        String sessionURL = baseURL+"Session;ACCESS_MODE_DATA=r;DB_CLOSE_DELAY=-1";
        session = RBB.connect(sessionURL);

        // now select the timeseries in the coordination RBB
        String coordinationURL = baseURL+"Coordination;DB_CLOSE_DELAY=-1";
        RBB coordinationRBB = RBB.create(coordinationURL, null);

        // select the input data and correct time to add a positive training example with the label "1"
        RBBSelection selection = new RBBSelection(session, coordinationRBB);
        // select sin first.  In this case it is significant because its sample
        // times will be used to observe the the problem, and the type=fixed
        // timeseries are only ever sampled once.
        // Alternately we could specify -timeStep 0.5 for the prediction step.
        selection.selectEvents(byID(sin.getID()));
        selection.selectEvents(byID(top.getID()));
        // at 2*pi, sin is at the top and headed to the right.
        // no predictionsRBB is specified because it is not necessary.
        Double t = 2*Math.PI;
        RBBML.RBBMLMain("setSimTime", "-coordinationRBB", coordinationURL, t.toString());

        String modelURL = baseURL+"Model;DB_CLOSE_DELAY=-1";
        RBBMLMain("create", "-modelRBB", modelURL, "gov.sandia.rbb.ml.models.TwoEntityModel", "mymodel");

        RBBML.RBBMLMain("train", "-sessionRBB", sessionURL, "-modelRBB", modelURL, "-coordinationRBB", coordinationURL, "1");

        // Now add a negative example a little later.
        t = 2.1*Math.PI;
        RBBML.RBBMLMain("setSimTime", "-coordinationRBB", coordinationURL, t.toString());
        RBBMLMain("train", "-sessionRBB", sessionURL, "-modelRBB", modelURL, "-coordinationRBB", coordinationURL, "-1");
        modelURL = baseURL+"Model;DB_CLOSE_DELAY=-1;ACCESS_MODE_DATA=r";


        RBBMLMain("print", "-rbb", sessionURL, "-modelRBB", modelURL);

        // now we will start predictions and need an RBB for that.
        String predictionsURL = baseURL+"Predictions;DB_CLOSE_DELAY=-1";
        RBB predictionsRBB = RBB.create(predictionsURL, null);

        // find where sin (blue) is near either fixed point - top or bottom.
        // for this we don't need a coordination RBB
        RBBMLMain("predict", "-sessionRBB", sessionURL, "-modelRBB", modelURL, "-predictionsRBB", predictionsURL, "-inputs", "description=sin", "type=fixed");

        // the circle goes around once each twopi, so passes the top or bottom 4 times in twopi
        // getting predictions requires the predictions RBB where the predictions are stored,
        // plus the session and model since it will only get results from that particular pairing.
        RBBML ml = new RBBML();
        ml.setRBBs("-predictionsRBB", predictionsURL, "-sessionRBB", sessionURL, "-modelRBB", modelURL);
        Event[] flags = ml.getPredictions();
        assertEquals(4, flags.length);
        // the duration of the flags shouldn't be super-long; in that case they'd be useless.
        for(Event flag : flags)
            assertTrue(flag.getEnd()-flag.getStart() < 0.5);

        //// try nextFlag, prevFlag
        // go to before the first flag.
        // for this we need coordinationRBB as read/write
        coordinationURL = baseURL+"Coordination;DB_CLOSE_DELAY=-1";
        /// ensure command-line setSimTime works.
        //RBBReplayControl.setReplayState(session, ReplayState.stoppedAtTime(0L));
        RBBMLMain("setSimTime", "-coordinationRBB", coordinationURL, "0");
        // unselect all Events.
        selection.deselectAll();
        RBBReplayControl replayControl = new RBBReplayControl(coordinationRBB, null, null);
        // Make sure the events were actually de-selected
        assertEquals(0, selection.getSelectedEventIDs().length);
        // prevFlag before first flag has no effect.
        // prevFlag requires predictionsRBB (read-only) and coordinationRBB, and modelRBB because it goes to the next flag for that model only.
        predictionsURL = baseURL+"Predictions;DB_CLOSE_DELAY=-1;ACCESS_MODE_DATA=r";
        RBBMLMain("prevFlag", "-predictionsRBB", predictionsURL, "-coordinationRBB", coordinationURL, "-modelRBB", modelURL);
        Thread.currentThread().sleep(100); // replayControl is updated asynchronously
        assertTrue(0L == replayControl.getSimTime());
        // first flag at PI
        RBBMLMain("nextFlag", "-predictionsRBB", predictionsURL, "-coordinationRBB", coordinationURL, "-modelRBB", modelURL);
        Thread.currentThread().sleep(100); // replayControl is updated asynchronously
        System.err.println("Time now "+replayControl.getSimTime()/1000.0);
        assertTrue(0.5 > Math.abs(Math.PI-replayControl.getSimTime()/1000.0));
        // going to the flag selects the inputs (i..e the events that were part of the flagged problem instance)
        session = RBB.connect(sessionURL);
        Event[] selectedEvents = Event.getByIDs(session.db(), selection.getSelectedEventIDs());
        assertEquals(2, selectedEvents.length);
        // the inputs are selected in the same order they were originally.
        assertEquals("sin", selectedEvents[0].getTagset().getValue("description"));
        assertEquals("bottom", selectedEvents[1].getTagset().getValue("description"));
        // next at 2*PI
        RBBMLMain("nextFlag", "-predictionsRBB", predictionsURL, "-coordinationRBB", coordinationURL, "-modelRBB", modelURL);
        Thread.currentThread().sleep(100); // replayControl is updated asynchronously
        assertTrue(0.5 > Math.abs(2*Math.PI-replayControl.getSimTime()/1000.0));
        // next at 3*PI
        RBBMLMain("nextFlag", "-predictionsRBB", predictionsURL, "-coordinationRBB", coordinationURL, "-modelRBB", modelURL);
        Thread.currentThread().sleep(100); // replayControl is updated asynchronously
        assertTrue(0.5 > Math.abs(3*Math.PI-replayControl.getSimTime()/1000.0));
        // next at 4*PI
        RBBMLMain("nextFlag", "-predictionsRBB", predictionsURL, "-coordinationRBB", coordinationURL, "-modelRBB", modelURL);
        Thread.currentThread().sleep(100); // replayControl is updated asynchronously
        assertTrue(0.5 > Math.abs(4*Math.PI-replayControl.getSimTime()/1000.0));
        // that's the last one, so further advancing has no effect.
        RBBMLMain("nextFlag", "-predictionsRBB", predictionsURL, "-coordinationRBB", coordinationURL, "-modelRBB", modelURL);
        Thread.currentThread().sleep(100); // replayControl is updated asynchronously
        assertTrue(0.5 > Math.abs(4*Math.PI-replayControl.getSimTime()/1000.0));
        // prevFlag goes back to 3*PI
        RBBMLMain("prevFlag", "-predictionsRBB", predictionsURL, "-coordinationRBB", coordinationURL, "-modelRBB", modelURL);
        Thread.currentThread().sleep(100); // replayControl is updated asynchronously
        assertTrue(0.5 > Math.abs(3*Math.PI-replayControl.getSimTime()/1000.0));

        // now try the stored prediction inputs.
        // need the predictions rbb read/write again for this.  Also needs session and model since stored prediction paramaters are specific to model/session pair.
        predictionsURL = baseURL+"Predictions;DB_CLOSE_DELAY=-1";
        RBBMLMain("storePredictionInputs", "-predictionsRBB", predictionsURL, "-sessionRBB", sessionURL, "-modelRBB", modelURL, "description=sin", "type=fixed", "model=booger,description=Position_A.description,description=Position_B.description");
        RBBMLMain("predict", "-sessionRBB", sessionURL, "-modelRBB", modelURL, "-predictionsRBB", predictionsURL, "-deleteOldResults", "-storedInputs");
        RBBMLMain("print",  "-sessionRBB", sessionURL, "-modelRBB", modelURL, "-predictionsRBB", predictionsURL);
        flags = Event.find(predictionsRBB.db(), byTags(ml.getModel().getMinimalResultTags()));
        assertEquals(4, flags.length);

        // now use a more general stored prediction input.
        // This will create additional results, but only 4 more, because the first 4 are created first by the previous stored inputs.
        RBBMLMain("storePredictionInputs","-predictionsRBB", predictionsURL, "-sessionRBB", sessionURL, "-modelRBB", modelURL, "type=dynamic", "type=fixed", "description=Position_A.description,description=Position_B.description");
        RBBMLMain("predict", "-sessionRBB", sessionURL, "-modelRBB", modelURL, "-predictionsRBB", predictionsURL, "-deleteOldResults", "-storedInputs");
        flags = Event.find(predictionsRBB.db(), byTags(ml.getModel().getMinimalResultTags()));
        assertEquals(8, flags.length);

        // now take the heading of the moving entity into account.
        // Only situations where a thing is moving right will be found.
  //      RBBMLMain("addFeatures", "-rbb", sessionURL, "-modelRBB", modelURL, "Heading_A");
  //      MLMain(new String[]{"predict", sessionURL, modelURL, "-deleteOldResults", "-flag", "-storeEvents", "", "type=dynamic", "type=fixed");
  //      MLMain(new String[]{"predict", sessionURL, modelURL, "-plot", "-outputFeature", "Range", "type=dynamic", "type=fixed", "-algorithm", "Default");
        
//        System.err.println("    calling print");
//        MLMain(new String[]{"print", sessionURL, modelURL});
//        flags = session.findEvents(model.getResultTags(), null,null,null,null);
//        assertEquals(4, flags.length);

    }


}