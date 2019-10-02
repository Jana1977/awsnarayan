package nd

import com.databricks.spark.xml._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

//File -> Settings -> Build, Execution, Deployment -> Build Tools -> SBT, check the "Download sources and Docs". open the SBT panel, and click refresh. Voila.
object MyTry {

  def main(args: Array[String]): Unit = {
    var filePath = "/home/serrorist/IdeaProjects/other_projects/MaskXmlData/src/main/scala/nd/xml/*"

    val sparkSession = SparkSession.builder.config("spark.master", "local").getOrCreate()

    val df = sparkSession.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "pnr")
      .xml(filePath)

    df.show()

    df.printSchema()

    import org.apache.spark.sql.functions._
    import sparkSession.implicits._

    //"Travelers.Traveler.firstName"


    var maskedDF: DataFrame = df

    // maskedDF = maskedDF.withColumn("Travelers", struct(struct(lit(maskValue($"Travelers.Traveler.firstName")).as("firstName"),
    //  col("Travelers.Traveler.middleName").as("middleName")).as("Traveler")))
    // maskedDF = maskedDF.withColumn("Travelers", struct(struct(lit(maskValue($"Travelers.Traveler.middleName")).as("middleName"), col("Travelers.Traveler.firstName").as("firstName")).as("Traveler")))


    /*
    <PNRid>999999999999</PNRid>
    <recordLocator>OJPRQL</recordLocator>
    <GDS>6</GDS>
    <platformID>NA</platformID>
    <BookingDateTime>9/6/2018 9:54:00 PM</BookingDateTime>
    <etrHits>0</etrHits>
    <Travelers>
        <Traveler>
            <firstName>WEI</firstName>
            <middleName>SEONG</middleName>
        </Traveler>
    </Travelers>
    <Segments>
        <Segment  xsi:type="AirSegment">
            <segmentNumber>1</segmentNumber>
            <TDSValidated>2</TDSValidated>
          </Segment>
    </Segments>
     */


    // -cols PNRid,recordLocator,GDS

    var arggs: Array[String] = new Array[String](2)
    arggs(0) = "-cols"
    arggs(1) = "PNRid,recordLocator,GDS,aa,Travelers.Traveler.middleName,Travelers.Traveler.firstName,Segment.segmentNumber"

    val maskValue = udf((s: String) => {
      s.replaceAll("\\S", "x")
    })

    for (field <- arggs(1).split(",")) {
      if (!field.contains(".") && maskedDF.schema.fieldNames.contains(field))
        maskedDF = maskedDF.withColumn(field, maskValue(col(field)))
      else if (field.contains(".")) {
        try {
          val nest = field.split("\\.")
          if (nest.length == 2) {
            val first = nest(0)
            val second = nest(1)
            val colStruct = df.select(col(first + ".*")).columns
              .filter(_ != second)
              .map(name => col(first + "." + name))
            maskedDF = maskedDF.withColumn(first,
              struct((colStruct :+ maskValue(col(field)).as(second)): _*)
            )
          }
          else if (nest.length == 3) {
            val first = nest(0)
            val second = nest(1)
            val third = nest(2)
            val colStruct = df.select(col(first + "." + second + ".*")).columns
              .filter(_ != third)
              .map(name => col(first + "." + second + "." + name))
            maskedDF = maskedDF.withColumn(first,
              struct(
                struct(
                  (colStruct :+ maskValue(col(field)).as(third)): _*
                ).as(second)
              )
            )
          }
        } catch {
          case e1: org.apache.spark.sql.AnalysisException => {
            println(e1)
          }
        }
      }
    }





    //maskedDF.show()
    //maskedDF.printSchema()


    maskedDF.repartition(1).write
      .format("com.databricks.spark.xml")
      .option("rootTag", "pnrs")
      .option("rowTag", "pnr")
      .mode(SaveMode.Overwrite)
      .xml("output_xml")
  }
}