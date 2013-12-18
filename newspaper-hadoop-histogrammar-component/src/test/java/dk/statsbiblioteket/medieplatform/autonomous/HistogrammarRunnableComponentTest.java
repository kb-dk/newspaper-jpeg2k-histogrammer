package dk.statsbiblioteket.medieplatform.autonomous;

import dk.statsbiblioteket.medieplatform.hadoop.HistogrammarJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.testng.Assert.assertTrue;

public class HistogrammarRunnableComponentTest {


    @Test(groups = "integrationTest")
    public void testDoWorkOnBatch() throws Exception {
        String pathToProperties = System.getProperty("integration.test.newspaper.properties");
        Properties properties = new Properties();
        properties.load(new FileInputStream(pathToProperties));

        Batch batch = new Batch("400022028241");

        properties.setProperty(
                ConfigConstants.JOB_FOLDER, "inputFiles-histogrammar-cibuild");
        properties.setProperty(
                ConfigConstants.PREFIX,
                "/net/zone1.isilon.sblokalnet/ifs/archive/bitmag-devel01-data/cache/avisbits/perm/avis/");
        properties.setProperty(ConfigConstants.HADOOP_USER, "newspapr");
        properties.setProperty(ConfigConstants.KAKADU_PATH, "kdu_expand -num_threads 4 -fprec 8M");
        properties.setProperty(ConfigConstants.FILES_PER_MAP_TASK, "5");
        properties.setProperty(HistogrammarJob.TEMP_FOLDER,"/tmp/");

        /*Standard hadoop properties you can play with*/
        properties.setProperty("mapreduce.map.speculative","false");
        properties.setProperty("mapreduce.job.reduces","2");

        clean(properties.getProperty(ConfigConstants.JOB_FOLDER));

        HistogrammarRunnableComponent component = new HistogrammarRunnableComponent(properties);
        ResultCollector resultCollector = new ResultCollector("crap", "crap");

        component.doWorkOnBatch(batch, resultCollector);
        assertTrue(resultCollector.isSuccess(), resultCollector.toReport());
    }

    private void clean(String jobFolder) throws IOException, InterruptedException {
        Configuration conf = new Configuration(true);
        String user = conf.get(ConfigConstants.HADOOP_USER, "newspapr");
        FileSystem fs = FileSystem.get(FileSystem.getDefaultUri(conf), conf, user);
        fs.delete(new Path(jobFolder), true);
    }

}
