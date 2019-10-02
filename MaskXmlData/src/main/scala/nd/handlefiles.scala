// SK's approach to handle the zipfiles separately as Scala code. This is scala ccode submitted using SPARK-SUBMIT and throws the error
// "ERROR ApplicationMaster: Uncaught exception: java.util.concurrent.TimeoutException: Futures timed out after [100000 milliseconds] "
// ----> so couldnt process ZIP files in bulk - there is a limit of 5 or 6.

//once the above project works - may be I have to bring it under this same project - need to know how?


package nd

import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
//import scala.io.Source.fromFile


object handlefiles extends App {

  val rootFolder = "/landing/data/hs/tsnt/froff/bdu/acbdustore/INPUT/zipfiles"
  //val rootFolder = "/tmp/tempbu/"
  //val rootFolder = "H:\\1_BCDHOME\\4_BDU-RCG\\94_BDU-DJ-TEST-docs\\SampleRecords\\FILES-used-for CDC test\\.zip"

  val conf = new Configuration()
  val path = new Path(rootFolder)

  // print the path or print the files? find out testing
  FileSystem.get(path.toUri, conf)
    .listStatus(path).foreach(x => println(x.getPath.toString))

  // filter files which has only ZIP extension.
  FileSystem.get(path.toUri, conf)
    .listStatus(path)
    .filter(_.getPath.getName.endsWith(".zip"))

    .foreach(file => {
      val parentPath = file.getPath.getParent.toString
      val newFileName = parentPath + "/xml/" + file.getPath.getName.replaceAll(".zip", "")
      //landing/data/hs/tsnt/froff/bdu/acbdustore/INPUT/zipfiles/xml/BCD_20180906_015743_236

      println(newFileName)
      println(file.getPath.toString)
      println(parentPath)

      FileUtil.unZip(
        //new File(file.getPath.toString.replace("maprfs://", "")),
        //new File(newFileName.replace("maprfs:", "")))
        
        new File(file.getPath.toString.replace("file://", "")),
        new File(newFileName.replace("file:", "")))

    })


}
