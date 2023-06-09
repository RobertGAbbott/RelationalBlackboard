/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservation.Metadata;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.LinkedList;

/**
 *
 * This feature extractor executes a process once for each problem instance.
 * The values of the inputs are sent to stdin of the process in the format:
 * t,f1d1,f1d2,f2d1,f2d2
 * Where t is the time, f1 is feature1, d1 is dimension 1, and so on.
 *
 * The command must write to standard output:
 * d1,d2... 
 *
 * @author rgabbot
 */
public class ExecFE extends MLFeatureExtractor {

    String[] inputs;
    Process proc;
    String cmd;
    BufferedReader is;
    LinkedList<MLObservation> q;

    public ExecFE(String output, String input, String cmd, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor, output);
        inputs = new String[1];
        inputs[0] = input;
        this.cmd = cmd;
    }

    @Override
    public void init(double time, Metadata md) throws Exception {
        System.err.println("exec "+cmd);
        proc = Runtime.getRuntime().exec(cmd);
        is = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        super.init(time, md);
        q = new LinkedList<MLObservation>();
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        OutputStream os = proc.getOutputStream();
        os.write(obs.getTime().toString().getBytes());
        for(String input : inputs) {
            for(Float x0 : obs.getFeatureAsFloats(input)) {
                os.write(",".getBytes());
                os.write(x0.toString().getBytes());
            }
        }
        os.write("\n".getBytes());
        os.flush();

        q.addLast(obs);

        drain();
    }

    private void drain() throws Exception {
        while(is.ready()) {
            String[] a = is.readLine().split(",");
            Float[] x = new Float[a.length];
            for(int i = 0; i < x.length; ++i)
                x[i] = Float.parseFloat(a[i]);
            MLObservation obs = q.pollFirst();
            obs.setFeatureAsFloats(super.getOutputName(0), x);
            super.observe(obs);
        }
    }

    @Override
    public void done(double time) throws Exception {
        proc.getOutputStream().close();
        while(!q.isEmpty()) {
            drain();
            System.err.println("sleeping");
            Thread.currentThread().sleep(100);
        }
        System.err.println("waitfor");
        proc.waitFor();
        super.done(time);
    }

}
