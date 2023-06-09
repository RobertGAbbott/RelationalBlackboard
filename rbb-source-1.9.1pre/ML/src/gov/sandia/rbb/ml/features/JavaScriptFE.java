
package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

/**
 *
 * This feature extractor supports executing javascript code to process other
 * inputs.
 *
 * The values of all inputs with non-null values are available as variables
 * in the javascript environment.
 *
 * See examples in JavaScriptFETest.java
 *
 * @author rgabbot
 */
public class JavaScriptFE extends MLFeatureExtractor {
    ScriptEngine scriptEngine;
    String expression;

    public JavaScriptFE(String output, String expression, MLFeatureExtractor nextFeatureExtractor) {
        super(nextFeatureExtractor, output);
        ScriptEngineManager mgr = new ScriptEngineManager();
        scriptEngine = mgr.getEngineByName("JavaScript");
        this.expression = expression;
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        scriptEngine.put("time", obs.getTime());
        for(String feature : obs.getFeatureNames()) {
            Object value = obs.getFeature(feature);
            // System.err.println("JavaScriptFE.observe: setting "+feature+" to "+value);
            if(value == null)
                continue;
            scriptEngine.put(feature, value);
        }
        Object result = scriptEngine.eval(expression);
        
        obs.setFeatureAsFloats(super.getOutputName(0), Float.parseFloat(result.toString()));
        super.observe(obs);
    }

}
