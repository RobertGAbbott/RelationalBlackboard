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
public class JavaScriptFETest {

    @Test
    public void javaScriptFETest() throws Exception {

        JavaScriptFE fe = new JavaScriptFE("y", "x[0]*x[0]", null);

        Metadata md = new Metadata();
        md.add("x", null);
        fe.addSelfToFeatureNames(md);

        fe.init(0.0, md);

        MLObservation obs;

        obs = new MLObservation(1.0, md);
        obs.setFeatureAsFloats("x", 2.0f);
        fe.observe(obs);
        assertEquals(4.0f, obs.getFeatureAsFloats("y")[0], 1e-6f);
    }

    @Test
    public void javaScriptFEAdditionTest() throws Exception {

        JavaScriptFE fe = new JavaScriptFE("y", "Number(x[0])+Number(x[1])", null);

        Metadata md = new Metadata();
        md.add("x", null);
        fe.addSelfToFeatureNames(md);

        fe.init(0.0, md);

        MLObservation obs;

        obs = new MLObservation(1.0, md);
        obs.setFeatureAsFloats("x", 2.0f, 3.0f);
        fe.observe(obs);
        assertEquals(5.0f, obs.getFeatureAsFloats("y")[0], 1e-6f);
    }

    @Test
    public void javaScriptFETimeTest() throws Exception {

        JavaScriptFE fe = new JavaScriptFE("y", "2*time", null);

        Metadata md = new Metadata();
        fe.addSelfToFeatureNames(md);

        fe.init(0.0, md);

        MLObservation obs;

        obs = new MLObservation(3.0, md);
        fe.observe(obs);
        assertEquals(6.0f, obs.getFeatureAsFloats("y")[0], 1e-6f);

    }

    @Test
    public void javaScriptFEVectorTest() throws Exception {

        JavaScriptFE fe = new JavaScriptFE("dist", "Math.sqrt(Math.pow(x[0]-y[0],2)+Math.pow(x[1]-y[1],2))", null);

        Metadata md = new Metadata();
        md.add("x", null);
        md.add("y", null);
        fe.addSelfToFeatureNames(md);

        fe.init(0.0, md);

        MLObservation obs;

        obs = new MLObservation(1.0, md);
        obs.setFeatureAsFloats("x", 10.0f, 20.0f);
        obs.setFeatureAsFloats("y", 13.0f, 24.0f); // make a 3/4/5 triangle
        fe.observe(obs);
        assertEquals(5.0f, obs.getFeatureAsFloats("dist")[0], 1e-6f);

    }

}