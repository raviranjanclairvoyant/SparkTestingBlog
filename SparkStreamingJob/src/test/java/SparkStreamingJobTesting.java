import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;


public class SparkStreamingJobTesting {



    private static SparkSession sparkSession;

    @Before
    public  void setUp() throws Exception {

        sparkSession = SparkSession.builder()
                .appName("SparkStructuredStreamingTesting")
                .master("local[*]")
                .getOrCreate();
    }





    @Test
   public void testSetupProcessing() throws StreamingQueryException {
        Dataset<Row> streamData = createInputData(sparkSession);

        streamData = SparkStreamingJobApplication.processStreamingData(sparkSession,streamData);
        List<Row> result=processData(streamData,sparkSession);
        System.out.println(result);

    }


    public static Dataset<Row> createInputData(SparkSession sparkSession) {

        String path = SparkStreamingJobTesting.class.getResource("InputData.csv").getPath();
        return sparkSession.readStream().
                format("csv").option("header", "true")
                .schema(new StructType()
                        .add("ID", DataTypes.IntegerType)
                        .add("Name", DataTypes.StringType)
                        .add("age", DataTypes.IntegerType)
                        .add("Occupation", DataTypes.StringType)
                        .add("Salary", DataTypes.IntegerType)
                        .add("Address", DataTypes.StringType))
                .load("/Users/ravranjan/Documents/SparkTestingBlog/SparkStreamingJob/src/test/resources/");

    }


    private static List<Row> processData(Dataset<Row> streamData,SparkSession sparkSession) throws StreamingQueryException {
        streamData.writeStream()
                .outputMode(OutputMode.Update()).format("memory").queryName("Output").start().processAllAvailable()

        ;
        return sparkSession.sql("select * from Output").collectAsList();

    }




}
