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
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> line1 = sc.parallelize(Arrays.asList("1 aa", "2 bb", "4 cc", "3 dd"));

        SQLContext sqlContext = new SQLContext(sc);

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
        sqlContext.createDataFrame(lineRowRDD,  DataTypes.createStructType(structFields)).show();

        prdd.foreach(new VoidFunction<Tuple2<String, String>>() {
            public void call(Tuple2<String, String> x) throws Exception {
                System.out.println(x);
            }
        });

        prdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, String> next = tuple2Iterator.next();
                    System.out.println(next);
                }
            }
        });

        sc.stop();

////        mainAlarmMap.foreachPartition(new VoidFunction<Iterator<Tuple2<String, List<Map<String, Long>>>>>() {
////
////            /**
////             *
////             */
////            private static final long serialVersionUID = 1L;
////
//            public void call(final Iterator<Tuple2<String, List<Map<String, Long>>>> t) throws Exception {
//                // TODO Auto-generated method stub
//                while (t.hasNext()) {
//                    Callable<Object> task = new Callable<Object>() {
//                        public Object call() {
////							Tuple2<String,List<Map<String,Long>>> alarmEntry = t.next();
//                            List<Map<String, Long>> timeList = t.next()._2();
//                            int mainAlarmNum = timeList.size();// 去重后，按支持度筛选的主告警发生数目
//                            String mainAlarmStr = t.next()._1();
//                            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                            for (int i = 0; i < timeList.size(); i++) {
//                                long firstTime = timeList.get(i).get("firstTime");
//                                String mainAlarmTime = sdf2.format(new Date(firstTime));
//                                firstTime = firstTime / 1000l - timeMargin * 60;
//                                long lastTime = timeList.get(i).get("lastTime") / 1000l + timeMargin * 60;
//                                StringBuffer subAlarmSelect = new StringBuffer();
//                                subAlarmSelect.append("select '").append(mainAlarmStr).append("' as mainAlarmStr,'")
//                                        .append(mainAlarmTime).append("' as mainAlarmTime,").append(mainAlarmNum)
//                                        .append(" as mainAlarmNum,")
//                                        .append("subAlarmStr,min(eventtime) as subAlarmTime from ")
//                                        .append(subAlarmSourceData)
//                                        .append(" where unix_timestamp(eventtime,'yyyy-MM-dd HH:mm:ss')>=")
//                                        .append(firstTime)
//                                        .append(" and unix_timestamp(eventtime,'yyyy-MM-dd HH:mm:ss')<=")
//                                        .append(lastTime).append(" group by '").append(mainAlarmStr).append("','")
//                                        .append(mainAlarmTime).append("','").append(mainAlarmNum)
//                                        .append("',subAlarmStr");
//                                DataFrame tmpSubDF = sqlContext.sql(subAlarmSelect.toString());
//                                tmpSubDF.registerTempTable("alarm_rule_base_table");
//                            }
//                            return "运行成功!";
//                        }
//                    };
//                    Future<Object> future = executor.submit(task);
//                    try {
//                        System.out.print(future.get());
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    } catch (ExecutionException e) {
//                        e.printStackTrace();
//                    }
//
//                }
//            }
//        });
    }
}
