package com.spark.sample.driver;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.jackson.map.ObjectMapper;
import scala.Tuple2;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public final class SkuToTermMR {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("SkuToTermMR")
                                             .setMaster("local[2]")
                                             .set("spark.executor.memory",
                                                  "1g");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        final JavaRDD<LinkedHashMap> datas = ctx.parallelize(parseSkuToTermFile());

        final JavaRDD<String> skuToTerms = datas.mapToPair(SKU_TO_BREAKDOWN_PAIR())
                                                .filter(RETAIN_VALID_SKUS())
                                                .flatMap(SKU_TERMS_PAIRS())
                                                .groupBy(GROUPBY_SKUID())
                                                .map(SKU_TO_TERMS());

        // .mapToPair(SKUID_TO_TERMS()); IF need to buidl Json
        FileUtils.deleteQuietly(new File("sku_to_terms"));
        skuToTerms.saveAsTextFile("sku_to_terms");
        ctx.stop();
    }

    private static Function<Tuple2<Object, Iterable<Tuple2<String, String>>>, String> SKU_TO_TERMS() {
        return new Function<Tuple2<Object, Iterable<Tuple2<String, String>>>, String>() {
            @Override
            public String call(Tuple2<Object, Iterable<Tuple2<String, String>>> input) throws Exception {
                List<String> searchTerms = Lists.newArrayList();
                for (Tuple2<String, String> terms : input._2()) {
                    searchTerms.add(terms._2());
                }
                return new StringBuilder(input._1()
                                              .toString()).append("->")
                                                          .append(Joiner.on(" ")
                                                                        .join(searchTerms))
                                                          .toString();
            }
        };
    }

    private static PairFunction<Tuple2<Object, Iterable<Tuple2<String, String>>>, String, List<String>> SKUID_TO_TERMS() {
        return new PairFunction<Tuple2<Object, Iterable<Tuple2<String, String>>>, String, List<String>>() {
            @Override
            public Tuple2<String, List<String>> call(Tuple2<Object, Iterable<Tuple2<String, String>>> input) throws Exception {

                List<String> searchTerms = Lists.newArrayList();
                for (Tuple2<String, String> terms : input._2()) {
                    searchTerms.add(terms._2());
                }
                return new Tuple2<String, List<String>>(input._1()
                                                             .toString(),
                                                        searchTerms);
            }
        };
    }

    private static Function<Tuple2<String, String>, Object> GROUPBY_SKUID() {
        return new Function<Tuple2<String, String>, Object>() {
            @Override
            public Object call(Tuple2<String, String> input) throws Exception {
                return input._1();
            }
        };
    }

    private static List parseSkuToTermFile() throws java.io.IOException {
        final HashMap skuToTermReport = MAPPER.readValue(new File("/Users/a6000401/Downloads/sku-to-term.json"),
                                                         HashMap.class);
        return (List<LinkedHashMap>) Lists.newArrayList(((LinkedHashMap) skuToTermReport.get("report")).get("data"))
                                          .get(0);
    }

    private static Function<Tuple2<String, List>, Boolean> RETAIN_VALID_SKUS() {
        return data -> !"::unspecified::".equals(data._1());
    }

    private static PairFunction<LinkedHashMap, String, List> SKU_TO_BREAKDOWN_PAIR() {
        return data -> new Tuple2(data.get("name")
                                      .toString(),
                                  (Lists.newArrayList(data.get("breakdown"))));
    }

    // Creating Tuple3 for future need of counts.If not can be conconverted to a Tuple2.
    private static FlatMapFunction<Tuple2<String, List>, Tuple2<String, String>> SKU_TERMS_PAIRS() {
        return new FlatMapFunction<Tuple2<String, List>, Tuple2<String, String>>() {
            @Override
            public Iterable<Tuple2<String, String>> call(Tuple2<String, List> input) throws Exception {
                List<Tuple2<String, String>> skuToSearchTerms = Lists.newArrayList();
                final String skuId = input._1();
                final List<LinkedHashMap> breakdowns = (List) input._2()
                                                                   .get(0);
                for (LinkedHashMap searchTermsForSku : breakdowns) {

                    final String searchTerm = (searchTermsForSku).get("name")
                                                                 .toString();
                    if ("::other::".equals(searchTerm) ||
                        skuId.equals(searchTerm)) {
                        continue;
                    }
                    final Integer counts = Integer.parseInt(((List) searchTermsForSku.get("counts")).get(0)
                                                                                                    .toString());
                    skuToSearchTerms.add(new Tuple2<String, String>(skuId,
                                                                    searchTerm.concat("|")
                                                                              .concat(String.valueOf(counts / 10000.0))));
                }
                return skuToSearchTerms;
            }
        };
    }
}
