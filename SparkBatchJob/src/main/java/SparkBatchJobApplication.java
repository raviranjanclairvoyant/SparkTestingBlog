import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class SparkBatchJobApplication {

    static SparkSession sparkSession = null;

    public static void main(String[] args) {

        sparkSession = SparkSession.builder()
                .appName("SparkBatchJob")
                .enableHiveSupport()
                .master("local[*]")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");

        Dataset<Row> targetDataFrame = execute(sparkSession);
        targetDataFrame.show();

    }

    public static Dataset<Row> execute(SparkSession sparkSession) {

        Dataset<Row> inputDataDf = createInputData(sparkSession);

        return inputDataDf;
    }

    public static Dataset<Row> createInputData(SparkSession sparkSession) {

        String path = SparkBatchJobApplication.class.getResource("InputData.csv").getPath();
        return sparkSession.read().
                format("csv").option("header", "true")
                .option("inferSchema", "true")
                .load(path);

    }

}
