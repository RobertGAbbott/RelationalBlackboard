package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservationSequence;
import gov.sandia.rbb.ml.ui.MLObservationPlot;
import java.util.ArrayList;
import org.jfree.chart.ChartPanel;
import org.jfree.ui.ApplicationFrame;


/**
 *
 * @author rgabbot
 */
public class ObservationPlotFE extends BufferObservationsFE {

    private String title, featureX, featureY, featureZ, nameTag;

    public ObservationPlotFE(String featureX, String featureY, String featureZ, String title, String nameTag, MLFeatureExtractor nextFE) {
        super(new ArrayList<MLObservationSequence>(), nextFE);
        this.title = title;
        this.featureX = featureX;
        this.featureY = featureY;
        this.featureZ = featureZ;
        this.nameTag = nameTag;
    }

    @Override
    public void batchDone() {
        try {
            final ApplicationFrame appFrame = new ApplicationFrame(title);
            MLObservationSequence[] seq = observationSequences.toArray(new MLObservationSequence[0]);
            ChartPanel chartPanel = MLObservationPlot.createInPanel(rbbml, seq, featureX, featureY, featureZ, nameTag);

            appFrame.add(chartPanel);
            appFrame.pack();
            appFrame.setVisible(true);
            super.batchDone();

        } catch(Exception e) {
            System.err.println(e.toString());
        }
    }



}
