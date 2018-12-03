package Model

import java.sql.{Connection, DriverManager, SQLException}

import org.slf4j.{Logger, LoggerFactory}

import com.typesafe.config.ConfigFactory


object AuroraUtil {
  implicit lazy val log: Logger = LoggerFactory.getLogger(this.getClass)

  val dbconfig = ConfigFactory.load()
  val auroraHost = dbconfig.getString("db.aurora.auroraHost")
  val auroraPort = dbconfig.getString("db.aurora.auroraPort")
  val auroraDatabase = dbconfig.getString("db.aurora.auroraDatabase")
  val auroraUser = dbconfig.getString("db.aurora.auroraUser")
  val auroraPassword = dbconfig.getString("db.aurora.auroraPassword")
  val auroraDriver = dbconfig.getString("db.aurora.auroraDriver")

  @volatile var connection: Connection = null
  val netsenseUrl = "jdbc:mysql://" + auroraHost + ":" + auroraPort + "/" + auroraDatabase

  def createConnection(): Connection = try {
    Class.forName(auroraDriver)
    println("Created db connection: " + auroraDatabase)

    if (connection == null) {
      connection = DriverManager.getConnection(netsenseUrl, auroraUser, auroraPassword)
      println("Created db connection: " + connection.isValid(0))
      connection

    } else {
      log.info("Reusing db connection: " + connection.isValid(0))
      connection
    }

  } catch {
    case ex: SQLException => log.error("Unable to establish connection " + ex.getMessage); throw ex
    case ex: Exception => log.error("Failed creating connection " + ex.getMessage); throw ex

  }


  def closeConnection(): Unit = {

    try {
      connection.close()

    } catch {

      case ex: Exception => log.warn("Failed closing connection: " + ex.getMessage)

    }

  }

}
