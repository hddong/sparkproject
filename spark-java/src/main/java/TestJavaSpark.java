import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class TestJavaSpark {
    public static transient JavaSparkContext sc;
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
        sc = new JavaSparkContext(conf);
        JavaRDD<String> line1 = sc.parallelize(Arrays.asList("1 aa", "2 bb", "4 cc", "3 dd"));

        SQLContext sqlContext = SQLContextSingleton.getInstance(sc);

        JavaPairRDD<String, String> prdd = line1.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String x) throws Exception {
                return new Tuple2(x.split(" ")[0], x.split(" ")[1]);
            }
        });
        JavaRDD<Row> lineRowRDD = line1.map(new Function<String, Row>() {
            public Row call(String line) throws Exception {
                String[] splited = line.split(" ");
                return RowFactory.create(splited[0], splited[1]);
            }
        });

        List structFields = new ArrayList();
        structFields.add(DataTypes.createStructField("search_word", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("lang", DataTypes.StringType, true));
        DataFrame df = sqlContext.createDataFrame(lineRowRDD,  DataTypes.createStructType(structFields));
        df.registerTempTable("test");
        df.show();
//        sqlContext.sql("select * from test").show();
        prdd.foreach(new VoidFunction<Tuple2<String, String>>() {
            public void call(Tuple2<String, String> x) throws Exception {
                System.out.println(x);
            }
        });

        SQLContextSingleton.getInstance(sc).sql("select * from test").show();
        prdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                DataFrame sql = SQLContextSingleton.getInstance(sc).sql("select * from test");
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, String> next = tuple2Iterator.next();
                    sql.show();
                    System.out.println(next);
                }
            }
        });

        sc.stop();
    }
}
