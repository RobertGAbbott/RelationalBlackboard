/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservation.Metadata;
import gov.sandia.rbb.ml.MLObservationSequence;
import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rgabbot
 */
public class PassFailFETest {
    @Test
    public void testEntersArea() throws Exception {

        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        // makes a feature extraction chain with one feature, "x", and a
        // PassFailFE.EntersArea between 1 and 2
        class Observer {
            MLFeatureExtractor fe;
            ArrayList<MLObservationSequence> results;
            Metadata md = new Metadata();
            Observer() {
                results = new ArrayList<MLObservationSequence>();
                md = new Metadata();
                md.add("x", null);
                fe = new PassFailFE.EntersArea("x", 1.0f, 2.0f,
                    new BufferObservationsFE(results, null));
            }
            void init(double t) throws Exception {
                fe.init(t, md);
            }
            void observe(double t, float x) throws Exception {
                MLObservation ob = new MLObservation(t, md);
                ob.setFeatureAsFloats("x", x);
                fe.observe(ob);
            }
            void done(double t) throws Exception {
                fe.done(t);
            }
        }
        Observer or = new Observer();
        or.init(0.5);
        or.observe(0.5, 0.6f);
        or.observe(2.4, 2.4f);
        or.done(2.5);
        assertEquals(0, or.results.size()); // never observed in area (even though it jumped over it)


        or = new Observer();
        or.init(0.5);
        or.observe(0.5, 0.6f);
        or.observe(1.5, 1.5f);
        or.observe(2.4, 2.4f);
        or.done(2.5);
        assertEquals(1, or.results.size());
        MLObservationSequence s = or.results.get(0);
        assertEquals(3, s.size());
        assertEquals(0.6f, s.getOldest().getFeatureAsFloats("x")[0], 1e-6f);
    }

    @Test
    public void testStartsIn() throws Exception {

        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        // makes a feature extraction chain with one feature, "x", and a
        // PassFailFE.StartsIn between 1 and 2
        class Observer {
            MLFeatureExtractor fe;
            ArrayList<MLObservationSequence> results;
            Metadata md = new Metadata();
            Observer() {
                results = new ArrayList<MLObservationSequence>();
                md = new Metadata();
                md.add("x", null);
                fe = new PassFailFE.StartsIn("x", 1.0f, 2.0f,
                    new BufferObservationsFE(results, null));
            }
            void init(double t) throws Exception {
                fe.init(t, md);
            }
            void observe(double t, Float x) throws Exception {
                MLObservation ob = new MLObservation(t, md);
                if(x!=null)
                    ob.setFeatureAsFloats("x", x);
                fe.observe(ob);
            }
            void done(double t) throws Exception {
                fe.done(t);
            }
        }

        // didn't start in area (even though it goes through it)
        Observer or = new Observer();
        or.init(0.5);
        or.observe(0.5, 0.5f);
        or.observe(1.5, 1.5f);
        or.observe(2.4, 2.4f);
        or.done(2.5);
        assertEquals(0, or.results.size());

        // does start in area
        or = new Observer();
        or.init(0.5);
        or.observe(1.5, 1.5f);
        or.observe(2.4, 2.4f);
        or.done(2.5);
        assertEquals(1, or.results.size());
        MLObservationSequence s = or.results.get(0);
        assertEquals(2, s.size());
        assertEquals(1.5f, s.getOldest().getFeatureAsFloats("x")[0], 1e-6f);

        // first observation null but second is in area
        or = new Observer();
        or.init(0.5);
        or.observe(1.4, null);
        or.observe(1.8, 1.8f);
        or.observe(2.4, 2.4f);
        or.done(2.5);
        assertEquals(1, or.results.size());
        s = or.results.get(0);
        assertEquals(3, s.size());

        // first observation null and second not in area but third is in area = fail
        or = new Observer();
        or.init(0.5);
        or.observe(1.4, null);
        or.observe(1.8, 2.8f);
        or.observe(2.4, 1.4f);
        or.done(2.5);
        assertEquals(0, or.results.size());
    }


    @Test
    public void testEndsIn() throws Exception {

        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        // makes a feature extraction chain with one feature, "x", and a
        // PassFailFE.StartsIn between 1 and 2
        class Observer {
            MLFeatureExtractor fe;
            ArrayList<MLObservationSequence> results;
            Metadata md = new Metadata();
            Observer() {
                results = new ArrayList<MLObservationSequence>();
                md = new Metadata();
                md.add("x", null);
                fe = new PassFailFE.EndsIn("x", 1.0f, 2.0f,
                    new BufferObservationsFE(results, null));
            }
            void init(double t) throws Exception {
                fe.init(t, md);
            }
            void observe(double t, Float x) throws Exception {
                MLObservation ob = new MLObservation(t, md);
                if(x!=null)
                    ob.setFeatureAsFloats("x", x);
                fe.observe(ob);
            }
            void done(double t) throws Exception {
                fe.done(t);
            }
        }

        // didn't end in area (even though it goes through it) = fail
        Observer or = new Observer();
        or.init(0.5);
        or.observe(0.5, 0.5f);
        or.observe(1.5, 1.5f);
        or.observe(2.4, 2.4f);
        or.done(2.5);
        assertEquals(0, or.results.size());

        // does end in area
        or = new Observer();
        or.init(0.5);
        or.observe(1.5, 2.5f);
        or.observe(2.4, 1.4f);
        or.done(2.5);
        assertEquals(1, or.results.size());
        MLObservationSequence s = or.results.get(0);
        assertEquals(2, s.size());

        // last observation null but second-to-last is in area = pass
        or = new Observer();
        or.init(0.5);
        or.observe(1.4, 1.4f);
        or.observe(1.8, 1.8f);
        or.observe(2.4, null);
        or.done(2.5);
        assertEquals(1, or.results.size());
        s = or.results.get(0);
        assertEquals(3, s.size());

        // lastt observation null and second-to-last not in area but third-to-last is in area = fail
        or = new Observer();
        or.init(0.5);
        or.observe(1.4, 1.4f);
        or.observe(1.8, 2.8f);
        or.observe(2.4, null);
        or.done(2.5);
        assertEquals(0, or.results.size());
    }

}
