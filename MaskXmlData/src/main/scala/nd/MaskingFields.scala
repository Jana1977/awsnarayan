// This code takes the UNZIPPED folders and created dataframes and add the filename to each record as a path.
// Then I am also hardcoding the needed PNRs to match across all of the files and then printing it out.

package nd

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

object ProcessExt extends App {

  val rootFolder = "/landing/data/hs/tsnt/froff/bdu/acbdustore/INPUT/zipfiles/xml/*"
  
  //PHASE1
  //MASKING and writing ZIP files back.

  // PHASE 2 
  // 5 nodes x 20 files = 100 files, 
  // all working in parallel


  // output expected 
  //  one file with all XML data merged in the FILENAME-TS-ORDER

  val spark = SparkSession.builder()
    .appName("ACBDUIngest")
    .getOrCreate()

  import spark.implicits._
  val df = spark.read.format("xml")
    .option("rowTag","pnr").load(rootFolder)
    .withColumn(colName = "fileName", input_file_name()) //2018-09-06 01-57-43Z-OJPRQL.xml //BCD_20180906_015743_236

  //df.createGlobalTempView("testbdudata")
  //df.filter($"Travelers.Traveler.firstName" === "JAXNAS").show(truncate = false)
  df.filter($"PNRid" === "338507065793479"  || $"PNRid" === "660981611043783"  || $"PNRid" === "961587638770647" || $"PNRid" ===  "583419084610466").show(truncate = false)
  //spark.sql("select 'jana' , PNRid from testbdudata")

  df.show(false)

}



