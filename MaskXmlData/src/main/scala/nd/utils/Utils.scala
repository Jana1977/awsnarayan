package nd.utils

import java.net.URI
import java.util.zip.{ZipEntry, ZipException, ZipInputStream, ZipOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.databricks.spark.xml._

import scala.sys.process.Process


object Utils {

  val configuration: Configuration = new Configuration()
  //val uri = Process("/usr/local/hadoop/bin/hdfs getconf -confKey fs.defaultFS").!!.trim
  val uri = configuration.get("fs.defaultFS")
  val hdfs: FileSystem = FileSystem.get(new URI(uri), configuration)

  def resolvePath(path: String): String = {
    hdfs.getHomeDirectory + "/" + path
  }


  def extractHdfsZipFile(source_zip: String, target_folder: String, sparksession: SparkSession): Boolean = {

    // println(resolvePath(source_zip))
    val buffer = new Array[Byte](1024)
    /*
     .collect -> run on driver only, not able to serialize hdfs Configuration
    */
    val zip_files = sparksession.sparkContext.binaryFiles(resolvePath(source_zip)).collect.
      foreach { zip_file: (String, PortableDataStream) =>
        // iterate over zip_files
        val zip_stream: ZipInputStream = new ZipInputStream(zip_file._2.open)
        var zip_entry: ZipEntry = null

        try {
          // iterate over all ZipEntry from ZipInputStream

          while ( {
            zip_entry = zip_stream.getNextEntry;
            zip_entry != null
          }) {
            // skip directory
            if (!zip_entry.isDirectory()) {
              //println(s"Extract File: ${zip_entry.getName()}, with Size: ${zip_entry.getSize()}")
              // create new hdfs file
              val hdfs_file: FSDataOutputStream = hdfs.create(new Path(resolvePath(target_folder) + "/" + zip_entry.getName()))

              var len: Int = 0
              // write until zip_stream is null
              while ( {
                len = zip_stream.read(buffer);
                len > 0
              }) {
                hdfs_file.write(buffer, 0, len)
              }
              // close and flush hdfs_file
              hdfs_file.close()
              hdfs_file.flush()
            }
            zip_stream.closeEntry()
          }
          zip_stream.close()
        } catch {
          case zip: ZipException => {
            println(zip.printStackTrace)
            println("Please verify that you do not use compresstype.")
            // for DEBUG throw exception
            //false
            throw zip
          }
          case e: Exception => {
            println(e.printStackTrace)
            // for DEBUG throw exception
            //false
            throw e
          }
        }
      }
    true
  }

  def maskXmlFiles(xml_path: String, masked_output_xml_path: String, sparkSession: SparkSession, fieldsToMask: String, rootTag: String): Unit = {
    val df = sparkSession.read
      .format("com.databricks.spark.xml")
      .option("rowTag", rootTag)
      .xml(resolvePath(xml_path))

    //.show()

    //df.printSchema()

    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    //"Travelers.Traveler.firstName"

    val maskValue = udf((s: String) => {
      s.replaceAll("\\S", "x")
    })

    var maskedDF: DataFrame = df

    for (field <- fieldsToMask.split(",")) {
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
              struct(
                (colStruct :+ maskValue(col(field)
                ).as(second)): _*)
            )
          } else if (nest.length == 3) {
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
          case e: org.apache.spark.sql.AnalysisException => {
            //println(e)
          }
        }
      }
    }

    maskedDF.repartition(1).write
      .format("com.databricks.spark.xml")
      .option("rootTag", rootTag + "s")
      .option("rowTag", rootTag)
      .mode(SaveMode.Overwrite)
      .xml(resolvePath(masked_output_xml_path))

    if (hdfs.exists(new Path(resolvePath(masked_output_xml_path + "/part-00000")))) {
      hdfs.rename(new Path(resolvePath(masked_output_xml_path + "/part-00000")), new Path(resolvePath(masked_output_xml_path + "/merged.xml")))
    }
  }


  def compressMaskedXmlAsZip(maskedXmlPath: String, maskedZippedXmlPath: String): Unit = {

    if (hdfs.exists(new Path(maskedZippedXmlPath))) {
      hdfs.delete(new Path(maskedZippedXmlPath), true)
    }

    val Buffer = 2 * 1024
    val data = new Array[Byte](Buffer)
    val zip = new ZipOutputStream(hdfs.create(new Path(maskedZippedXmlPath + "merged_xml.zip")))
    val mergedXmlFileName = maskedXmlPath + "merged.xml"
    zip.putNextEntry(new ZipEntry(mergedXmlFileName))
    val in: FSDataInputStream = hdfs.open(new Path(mergedXmlFileName))
    var b = in.read(data, 0, Buffer)
    while (b != -1) {
      zip.write(data, 0, b)
      b = in.read(data, 0, Buffer)
    }
    in.close()
    zip.closeEntry()
    zip.close()
  }

}
