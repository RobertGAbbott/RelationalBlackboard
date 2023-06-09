/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLObservation.Metadata;
import gov.sandia.rbb.ml.MLObservation;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rgabbot
 */
public class DerivativeFETest {

    @Test
    public void derivativeFETest() throws Exception {

        // "time" here is a pseudo-feature that refers to the observation time,
        // which is specific to DerivativeFE
        DerivativeFE fe = new DerivativeFE("pace", "time", "dist", 10.0f, null);

        Metadata md = new Metadata();
        md.add("dist", null);
        fe.addSelfToFeatureNames(md);

        fe.init(0.0, md);

        MLObservation obs;

        obs = new MLObservation(1000.0, md);
        obs.setFeatureAsFloats("dist", 100.0f);
        fe.observe(obs);
        assertNull(obs.getFeature("pace")); // can't compute pace until we've moved.

        obs = new MLObservation(1006.0, md);
        obs.setFeatureAsFloats("dist", 104.0f);
        fe.observe(obs);
        assertNull(obs.getFeature("pace")); // have only moved 4m, which is less than dx of 10m

        obs = new MLObservation(1018.0, md);
        obs.setFeatureAsFloats("dist", 114.0f);
        fe.observe(obs);
        // have moved
        // 8m  since 1006s, and
        // 14m since 1000s.
        // The required dx is 10m, 1/3 of the way between those.
        // so the estimated time is 1002s, which is now 12s ago.
        // taking 12s to move 10m is a pace of 1.2s/m
        assertEquals(1.2f, obs.getFeatureAsFloats("pace")[0], 1e-6);

        // increase time by 2 without increasing distance.
        obs = new MLObservation(1020.0, md);
        obs.setFeatureAsFloats("dist", 114.0f);
        fe.observe(obs);
        assertEquals(1.4f, obs.getFeatureAsFloats("pace")[0], 1e-6);

        // now, move 3 * dx in a single observation.
        obs = new MLObservation(1030.0, md);
        obs.setFeatureAsFloats("dist", 144.0f);
        fe.observe(obs);
        assertEquals(1.0f/3.0f, obs.getFeatureAsFloats("pace")[0], 1e-6);

    }

    @Test
    public void derivativeFETestUnspecifiedDx() throws Exception {

        DerivativeFE fe = new DerivativeFE("speed", "dist", null, null);

        Metadata md = new Metadata();
        md.add("dist", null);
        fe.addSelfToFeatureNames(md);

        fe.init(0.0, md);

        MLObservation obs;

        obs = new MLObservation(1000.0, md);
        obs.setFeatureAsFloats("dist", 100.0f);
        fe.observe(obs);
        assertNull(obs.getFeature("speed")); // can't compute speed until some time has elapsed.

        obs = new MLObservation(1002.0, md);
        obs.setFeatureAsFloats("dist", 106.0f);
        fe.observe(obs);
        assertEquals(3.0f, obs.getFeatureAsFloats("speed")[0], 1e-6f); // have only moved 4m, which is less than dx of 10m


    }


    @Test
    public void derivativeFETestVector() throws Exception {

        DerivativeFE fe = new DerivativeFE("speed", "dist", null, null);

        Metadata md = new Metadata();
        md.add("dist", null);
        fe.addSelfToFeatureNames(md);

        fe.init(0.0, md);

        MLObservation obs;

        obs = new MLObservation(1000.0, md);
        obs.setFeatureAsFloats("dist", 100.0f, 101.0f);
        fe.observe(obs);
        assertNull(obs.getFeature("speed")); // can't compute speed until some time has elapsed.

        obs = new MLObservation(1002.0, md);
        obs.setFeatureAsFloats("dist", 106.0f, 101.6f);
        fe.observe(obs);
        assertEquals(3.0f, obs.getFeatureAsFloats("speed")[0], 1e-6f); 
        assertEquals(0.3f, obs.getFeatureAsFloats("speed")[1], 1e-6f);


    }

}