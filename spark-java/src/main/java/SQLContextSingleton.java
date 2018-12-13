import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class SQLContextSingleton {
    private static transient SQLContext instance;
    public static SQLContext getInstance(JavaSparkContext sc) {
        if (instance == null) {
            instance = new SQLContext(sc);
        }
        return instance;
    }
}
