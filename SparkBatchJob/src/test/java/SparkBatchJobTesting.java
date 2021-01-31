import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;
import org.scalatest.FunSuite;
import com.holdenkarau.spark.testing.DataframeGenerator;
import com.holdenkarau.spark.testing.DatasetGenerator;
import com.holdenkarau.spark.testing.JavaDataFrameSuiteBase;
import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import com.holdenkarau.spark.testing.RDDGenerator;


public class SparkBatchJobTesting extends JavaDatasetSuiteBase {

    private static SparkSession sparkSession;
    private static Dataset<Row> testDataDF;

    @Before
    public void setUp()
    {
        sparkSession = SparkSession
                .builder()
                .appName("Testing")
                .enableHiveSupport()
                .master("local[*]")
                .getOrCreate();

        testDataDF = createTestData(sparkSession);

    }

    public static Dataset<Row> createTestData(SparkSession sparkSession) {

        String path = SparkBatchJobTesting.class.getResource("testData.csv").getPath();
        return sparkSession.read().
                format("csv").option("header", "true")
                .option("inferSchema", "true")
                .load(path);
    }

    @Test
    public void testInputDataFrameAndTestDataFrame() {

        SparkBatchJobApplication readCSV = new SparkBatchJobApplication();
        Dataset<Row>InputDF= readCSV.execute(sparkSession);

        assertDataFrameNoOrderEquals(InputDF,testDataDF);       // calling assert directly on dataframe
        //assertDataFrameEquals(InputDF,testDataDF);
        //assertDataFrameDataEquals(InputDF,testDataDF);
        //assertDataFrameApproximateEquals(InputDF,testDataDF,0);


    }









}
