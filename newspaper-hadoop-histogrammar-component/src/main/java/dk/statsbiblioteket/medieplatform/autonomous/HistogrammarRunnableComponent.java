package dk.statsbiblioteket.medieplatform.autonomous;

import dk.statsbiblioteket.medieplatform.hadoop.AbstractHadoopRunnableComponent;
import dk.statsbiblioteket.medieplatform.hadoop.HistogrammarJob;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class HistogrammarRunnableComponent extends AbstractHadoopRunnableComponent {

    private static Logger log = LoggerFactory.getLogger(HistogrammarRunnableComponent.class);


    /**
     * Constructor matching super. Super requires a properties to be able to initialise the tree iterator, if needed.
     * If you do not need the tree iterator, ignore properties.
     *
     * You can use properties for your own stuff as well
     *
     * @param properties properties
     *
     * @see #getProperties()
     */
    public HistogrammarRunnableComponent(Properties properties) {
        super(properties);
    }

    @Override
    protected Tool getTool() {
        return new HistogrammarJob();
    }

    /**
     * This is the event ID that correspond to the work done by this component. It will be added to the list of
     * events a batch have experienced when the work is completed (along with information about success or failure
     */
    @Override
    public String getEventID() {
        return "Histogrammed";
    }


}
