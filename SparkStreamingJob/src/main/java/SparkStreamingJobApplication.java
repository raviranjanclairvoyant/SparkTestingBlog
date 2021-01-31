import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;


public class SparkStreamingJobApplication {

    static SparkSession sparkSession = null;

    public static void main(String[] args) throws StreamingQueryException {


        sparkSession = SparkSession.builder()
                .appName("SparkStreamingJob")
                .master("local[*]")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
         execute(sparkSession);

    }

    public static void execute(SparkSession sparkSession) throws StreamingQueryException {


        Dataset<Row> streamData = sparkSession.readStream()
                .schema(new StructType()
                        .add("ID", DataTypes.IntegerType)
                        .add("Name", DataTypes.StringType)
                        .add("age", DataTypes.IntegerType)
                        .add("Occupation", DataTypes.StringType)
                        .add("Salary", DataTypes.IntegerType)
                        .add("Address", DataTypes.StringType))
                        .option("header",true)
                        .option("maxFilesPerTrigger",1)
                .csv("SparkStreamingJob/src/main/resources/StreamingData/*.csv");


        Dataset<Row> outputDf = processStreamingData(sparkSession, streamData);

        StreamingQuery query = outputDf.writeStream()
               .outputMode(OutputMode.Update()).format("console").start();

        query.awaitTermination();



    }




    public static Dataset<Row> processStreamingData(SparkSession sparkSession, Dataset<Row> csvDF) {
        csvDF.createOrReplaceTempView("employeeData");
     return
             sparkSession.sql("select count(*), Occupation from  employeeData group by Occupation");
    }



}
