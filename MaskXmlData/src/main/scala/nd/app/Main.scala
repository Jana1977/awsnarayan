package nd.app

import nd.utils.Utils
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.inf.{ArgumentParser, ArgumentParserException, Namespace}
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    val argumentParser: ArgumentParser = ArgumentParsers.newArgumentParser("Main", false)
      .description("Please place zipped xml files in path: " + Utils.resolvePath(ProcessLoader.zipFilesPath))

    argumentParser.addArgument("-rootTag")
      .dest("rootTag").metavar("")
      .required(true)
      .help("Please specify rootTag of xml file\nExample: -rootTag mainTag\n")

    argumentParser.addArgument("-fieldsToMask")
      .dest("fieldsToMask").metavar("")
      .required(true)
      .help("Please separate fields by comma without space as field1,field2\n" +
        "For nested fields, please join them by dot as Person.name.firstName,Person.name.lastName\n"
        + "Example: -fieldsToMask field1,field2,Person.name.firstName,Person.name.lastName,field3\n")

    var arguments: Namespace = null
    try {
      arguments = argumentParser.parseArgs(args)
    } catch {
      case e: ArgumentParserException => {
        argumentParser.printHelp()
        return
      }
    }

    val sparkSession = SparkSession.builder.config("spark.master", "yarn").appName("MaskXmlData").getOrCreate()

    ProcessLoader.initiateTasks(arguments, sparkSession)

    sparkSession.close()

    println()
    println("Process completed.")
    println("*****************************************************************************************")
    println("Dump locations:")
    println("Initial zip files:      " + Utils.resolvePath(ProcessLoader.zipFilesPath))
    println("Unzipped files:         " + Utils.resolvePath(ProcessLoader.unzippedFilesPath))
    println("Masked and merged file: " + Utils.resolvePath(ProcessLoader.maskedXmlPath))
    println("Zipped masked file:     " + Utils.resolvePath(ProcessLoader.zippedMaskedXmlPath))
    println("*****************************************************************************************")
    println()

  }

}
