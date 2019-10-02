package nd.app

import nd.utils.Utils
import net.sourceforge.argparse4j.inf.Namespace
import org.apache.spark.sql.SparkSession


object ProcessLoader {

  val zipFilesPath = "zipped_files/"
  val unzippedFilesPath = "unzipped_files/"
  val maskedXmlPath = "xml_masked/"
  val zippedMaskedXmlPath = "xml_masked_zipped/"

  def initiateTasks(arguments: Namespace, sparkSession: SparkSession): Unit = {

    val fieldsToMask = arguments.getString("fieldsToMask")
    val rootTag = arguments.getString("rootTag")

    // fieldsToMask => "PNRid,recordLocator,GDS,aa,Travelers.Traveler.middleName,Travelers.Traveler.firstName"

    /**
     * 1. unzipping zip files in hadoop
     */
    Utils.extractHdfsZipFile(zipFilesPath, unzippedFilesPath, sparkSession)

    /**
     * 2. Applying masking and merging
     */
    Utils.maskXmlFiles(unzippedFilesPath, maskedXmlPath, sparkSession, fieldsToMask, rootTag)

    /**
     * 3. zipping masked files
     */
    Utils.compressMaskedXmlAsZip(maskedXmlPath, zippedMaskedXmlPath)

  }

}
