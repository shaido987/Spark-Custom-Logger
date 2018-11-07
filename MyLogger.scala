import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Dataset
import org.apache.spark.rdd.RDD

/*
 * Extend Logging to use
 */
trait Logging extends Serializable {
    val loggerName = this.getClass.getName
    val logger = LogManager.getLogger(loggerName)
    logger.setLevel(MyLogLevel)
    
    private def print(message: String, prefix: String = "") = {
      val adjustedPrefix = if (prefix.isEmpty) prefix else prefix + ": " 
      logger.log(MyLogLevel, adjustedPrefix + message)
    }
    
    // Common log method for all datatypes and datastructures
    def log(input: Any, prefix: String = "", linesForTable: Int = 10): Unit = input match {
      case ds: Dataset[_]   => {
        val adjustedPrefix = if (prefix.isEmpty) "DataFrame/DataSet" else prefix
        print(adjustedPrefix + " (the first " + linesForTable + " lines):")
        print("Columns: " + ds.schema.fieldNames.mkString(", "))
        ds.take(linesForTable) foreach (row => print(row.toString))
      }
      case rdd: RDD[_]        => {
        val adjustedPrefix = if (prefix.isEmpty) "RDD" else prefix
        print(adjustedPrefix + " (the first " + linesForTable + " lines):")
        rdd.take(linesForTable) foreach (row => print(row.toString))
      }
      case schema: StructType => print(schema.prettyJson, prefix)
      case a: Array[_]        => print(a.toList.toString, prefix)
      case message            => print(message.toString, prefix)
    }
}

/*
 * Custom log level when logging to easier differentiate between the messages
 */
object MyLogLevel extends MyLevel(250, "LOGGER -", 7) with Serializable //Higher priority than WARN
class MyLevel(val logLevel: Int, val name: String, arg2: Int) extends Level (logLevel, name, arg2) with Serializable {
  
  def toLevel(sArg: String): Level = {
    if (sArg != null && sArg.toUpperCase() == name)
      MyLogLevel
    else
      toLevel(sArg)
  }
}
