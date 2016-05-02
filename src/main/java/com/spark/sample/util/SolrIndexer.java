package com.spark.sample.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by a6000401 on 5/1/16.
 */
public class SolrIndexer {

    private HttpSolrClient productsSolrClient = new HttpSolrClient("http://localhost:28001/solr/products");
    private static final Log LOG = LogFactory.getLog(SolrIndexer.class);

    public void addPayloadsToSkus(String skuid,
                                  String payload) throws IOException, SolrServerException {

        SolrInputDocument sdoc = new SolrInputDocument();
        sdoc.addField("skuid",
                      skuid);
        Map<String, Object> fieldModifier = new HashMap<>(1);
        fieldModifier.put("add",
                          payload);
        sdoc.addField("skuToTerm",
                      fieldModifier); // add the map as the field value

        productsSolrClient.add(sdoc); // send it to the solr server

    }

    public void commitProductSolrCore() throws Exception {
        LOG.info("!!!Commiting!!!");
        productsSolrClient.commit();
    }

    public static void main(String[] args) throws Exception {

        SolrIndexer indexer = new SolrIndexer();
        indexer.addPayloadsToSkus("4456600",
                                  "laptops|0.2571 lenovo flex 3|0.1761 2 in 1 laptop|0.1473 lenovo|0.1382 redirect: lenovo|0.0937 lenovo laptop|0.0791 lenovo flex|0.0773 lenovo flex 3 11|0.0722");
    }
}
