package com.spark.sample.app;

import com.google.common.base.Stopwatch;
import com.spark.sample.util.SolrIndexer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hello world!
 */
public class App {

    private static final Log LOG = LogFactory.getLog(App.class);

    private final SolrIndexer indexer = new SolrIndexer();
    private AtomicInteger totalSkuCount = new AtomicInteger();

    public void partialUpdateSolrSkuDocs(final String dataForIndexing) throws Exception {
        final List<String> skuToTerms = FileUtils.readLines(new File(dataForIndexing));
        for (String skuInfo : skuToTerms) {
            final String[] splits = skuInfo.split("->");
            indexer.addPayloadsToSkus(splits[0],
                                      splits[1]);
            totalSkuCount.incrementAndGet();
        }
        indexer.commitProductSolrCore();
        LOG.info("Total sku updated::" +
                 totalSkuCount.intValue());

    }

    public static void main(String[] args) {
        try {
            App app = new App();
            Stopwatch stopwatch = Stopwatch.createStarted();
            app.partialUpdateSolrSkuDocs("sku_to_terms/part-00000");
            app.partialUpdateSolrSkuDocs("sku_to_terms/part-00001");
            LOG.info("Completed !!!! Total time taken in seconds::" +
                     stopwatch.elapsed(TimeUnit.SECONDS));
            stopwatch.stop();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
}
