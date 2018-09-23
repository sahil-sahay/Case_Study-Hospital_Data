package SQL
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object HospitalDrugType {

  // Case classes does not work on this dataset. Throwing parse errors on 8th field.Need to find out the reason.
  //  case class Hospital(DRGDefinition:String, ProviderId:Long, ProviderName:String,	ProviderStreetAddress:String, ProviderCity:String,	ProviderState:String,	ProviderZipCode:String,	TotalDischarges:Long,	AverageCoveredCharges:Double,	AverageTotalPayments:Double,	AverageMedicarePayments:Double

  //Create a structure to be used globally inside the main method
  //Inferring the Schema Using Reflection.Automatically converting an RDD containing CustomSchemaHospital to a DataFrame.
  // The CustomSchemaHospital defines the schema of the table. The names of the fields to the CustomSchemaHospital
  // are read using reflection and become the names of the columns.

  val CustomSchemaHospital = new StructType(Array(
    StructField("DRGDefinition", StringType),
    StructField("ProviderId", LongType),
    StructField("ProviderName", StringType),
    StructField("ProviderStreetAddress", StringType),
    StructField("ProviderCity", StringType),
    StructField("ProviderState", StringType),
    StructField("ProviderZipCode", LongType),
    StructField("HospitalReferralRegionDescription", StringType),
    StructField("TotalDischarges", LongType),
    StructField("AverageCoveredCharges", DoubleType),
    StructField("AverageTotalPayments", DoubleType),
    StructField("AverageMedicarePayments", DoubleType))
  )

  // Main method - The execution entry point for the program
  def main(args: Array[String]): Unit  = {

    //Let us create a spark session object
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Hospital_Drug_Case-Study")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //Set the log level as warning
    spark.sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs and sequences  to DataFrames
    import spark.implicits._

    println("Spark Session Object created")
    /*
        val data = spark.sparkContext.textFile("C:\\Users\\ssahay\\Documents\\Acadgild\\Completed_Assignments\\Day_25_Interview_Prep\\inpatientCharges.csv")
        data.first().foreach(print)
        // Storing the first line that is header of the file into another variable
        val header = data.first()

        // Selecting all other lines into data1 excluding the header.
        val data1 = data.filter(row => row != header)

        //Converting data1 into dataFrame splitting on comma character (,)
        // trim is used to eliminate leading/trailing whitespace & converting values into respective data types
        val hospitalData =  data1.map(_.split(","))
          .map(x => Hospital(x(0),x(1).trim.toLong,x(2),x(3),x(4),x(5),x(6), x(8).trim.toLong, x(9).trim.toDouble, x(10).trim.toDouble, x(11).trim.toDouble))
          .toDF()

        hospitalData.collect*/
//    Objective -1 , Loading File into Spark
    //Specifying the format as csv for the input file being read. keeping the header in DataFrame
    //providing the input schema from the struct created above. Loading the csv file & converting same into DataFrame.
    val hospitalData = spark.read.format("csv")
      .option("header", "true")
      .schema(CustomSchemaHospital)
      .load("C:\\Users\\ssahay\\Documents\\Acadgild\\Completed_Assignments\\Day_25_Interview_Prep\\inpatientCharges.csv")
      .toDF()

    //displaying the dataFrame created above.
    hospitalData.show

    //Converting the above created schema into an SQL view named hospital
    hospitalData.createOrReplaceTempView("hospital")

//Objective -2   What is the average amount of AverageCoveredCharges per state???

    //Select providerState column, calculating average using avg aggregate function on the column AverageCoveredCharges
    //grouping by each state calculates average per state. Round function with 2 as a second parameter keeps only two values after decimal.
    val a2 = spark.sql("select ProviderState,round(avg(AverageCoveredCharges),2) as `Avg_Amount/State` from hospital group by ProviderState")

    //Displaying the result
    a2.show


//Objective -2    AverageTotalPayments charges per state
    //cast - Casts the value of AverageTotalPayments as decimal & dividing the result set by 100 & finding entire sum using aggregate function sum
    //grouping by each state calculates average per state. Round function with 2 as a second parameter keeps only two values after decimal.
    val a3 = spark.sql("select ProviderState,round(sum(cast(AverageTotalPayments as decimal)/cast(pow(10,2) as decimal)),2) as `Total_Payment/State` from hospital group by ProviderState")

    //Displaying the result
    a3.show


//Objective -2    AverageMedicarePayments charges per state.
//cast - Casts the value of AverageMedicarePayments as decimal & dividing the result set by 100 & finding entire sum using aggregate function sum
//grouping by each state calculates average per state. Round function with 2 as a second parameter keeps only two values after decimal.
val a4 = spark.sql("select ProviderState,round(sum(cast(AverageMedicarePayments as decimal)/cast(pow(10,2) as decimal)),2) as `Total_Medicare_Payment/State` from hospital group by ProviderState")

    //Displaying the result
    a4.show


//    Objective -3 Total number of Discharges per state and for each disease
    //Selecting DRGDefinition,ProviderState & total of TotalDischarges with alias #Discharges/State/Disease
    //grouping the result based on DRGDefinition,ProviderState
val a5 = spark.sql("select DRGDefinition,ProviderState, sum(TotalDischarges) as `#Discharges/State/Disease` from hospital group by DRGDefinition,ProviderState")

    //Displaying the result
    a5.show

//     Objective -3 Sort the output in descending order of totalDischarges
//Selecting DRGDefinition,ProviderState & total of TotalDischarges with alias #Discharges/State/Disease
//grouping the result based on DRGDefinition,ProviderState & ordering the result in descending order of sum(TotalDischarges)
val a6 = spark.sql("select DRGDefinition,ProviderState, sum(TotalDischarges) as `#Discharges/State/Disease` from hospital group by DRGDefinition,ProviderState order by `#Discharges/State/Disease` desc")

    //Displaying the result
    a6.show
  }
}
