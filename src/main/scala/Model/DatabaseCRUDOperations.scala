package Model

import java.sql.{SQLIntegrityConstraintViolationException, Statement}

import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{JsValue, Json, Writes}

//import tour.Helpers._
case class RulesReport(nodeids: Set[String],
                       alarmtypes: Set[String],
                       alertstatus: String,
                       reportcolumns: Set[String])

case class Report(reportId: String,
                  reportName: Option[String] = None, //"My Test Report"
                  reportType: Option[String] = Some("Alert Summary"), //Default: "Alert Summary" (or something else in Future)
                  siteId: String, //siteid
                  orgId: String, //orgid
                  timePeriod: Option[String] = Some("1day"), //default: "1day", "2days", "7days", "30 days", "60 days", "90days"
                  schedule: Option[String] = None, //Cron Entry based on Site Timezone //0 17 * * *
                  lastSent: Option[String] = None, //UTC String (Spark/Lamda Have to update it)
                  nextScheduled: Option[String] = None, //UTC String (Spark/Lamda Have to update it)
                  active: Option[String] = Some("false"), //Boolean
                  rules: Option[String] = None, //{"nodeids": ["N1","N2"], "alarmtypes": ["T1","T2"], "alertstatus": "active/inactive", "reportcolumns": ["nodeid","alarmtype"] }
                  userEmails: Option[String] = None, //Comma seperated list of email
                  created: Option[Long] = None, //Epoch Millis
                  updated: Option[Long] = None //Epoch Millis
                 )


case class ReportResponse(reportId: String,
                          reportName: String, //"My Test Report"
                          reportType: String, //Default: "Alert Summary" (or something else in Future)
                          siteId: String, //siteid
                          orgId: String, //orgid
                          timePeriod: String, //default: "1day", "2days", "7days", "30 days", "60 days", "90days"
                          schedule: String, //Cron Entry based on Site Timezone //0 17 * * *
                          lastSent: String, //UTC String (Spark/Lamda Have to update it)
                          nextScheduled: String, //UTC String (Spark/Lamda Have to update it)
                          active: String, //Boolean
                          rules: String, //{"nodeids": ["N1","N2"], "alarmtypes": ["T1","T2"], "alertstatus": "active/inactive", "reportcolumns": ["nodeid","alarmtype"] }
                          userEmails: String, //Comma seperated list of email
                          created: Long, //Epoch Millis
                          updated: Long //Epoch Millis
                         )

case class ResponseObj(status: String, message: String)

import spray.json.DefaultJsonProtocol

trait ResponseObjProcess extends DefaultJsonProtocol {
  implicit val ReportResponseFormat = jsonFormat14(ReportResponse.apply)

  implicit val ReportResponseWrites = new Writes[ReportResponse] {
    def writes(json_write: ReportResponse): JsValue = {
      Json.obj(
        "reportId" -> json_write.reportId,
        "reportType" -> json_write.reportType,
        "siteId" -> json_write.siteId,
        "orgId" -> json_write.orgId,
        "timePeriod" -> json_write.timePeriod,
        "schedule" -> json_write.schedule,
        "lastSent" -> json_write.lastSent,
        "nextScheduled" -> json_write.nextScheduled,
        "active" -> json_write.active,
        "rules" -> json_write.rules,
        "userEmails" -> json_write.userEmails,
        "created" -> json_write.created,
        "updated" -> json_write.updated,
        "reportName" -> json_write.reportName
      )
    }
  }
}


class DatabaseCRUDOperations extends ResponseObjProcess {


  implicit lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
  lazy val sqlConnection = AuroraUtil.createConnection()
  val mySQL: Statement = sqlConnection.createStatement()

  def insertRecords(tableName: String, input: Report): ReportResponse = {
    var primaryKey = 0
    val created_ts = System.currentTimeMillis()
    val insertColumns =
      s"""(report_id,report_name,report_type,site_id,org_id,
         |time_period,schedule,last_sent,next_scheduled,
         |active,rules,user_emails,created,updated)""".stripMargin

    val insertQuery =
      s"""
         |INSERT INTO $tableName $insertColumns
         |VALUES ('${input.reportId}', '${input.reportName.getOrElse("My Test Report")}', '${input.reportType.getOrElse("")}', '${input.siteId}',
         |'${input.orgId}','${input.timePeriod.getOrElse("1day")}','${input.schedule.getOrElse("")}','${input.lastSent.getOrElse("")}',
         |'${input.nextScheduled.getOrElse("")}','${input.active.getOrElse("false")}','${input.rules}','${input.userEmails.getOrElse("")}',
         |'${input.created.getOrElse(created_ts)}','${input.updated.getOrElse(0)}'
         | )
      """.stripMargin

    logger.info(s"GPS Aurora Query: $insertQuery")
    try {

      var rowAffected = mySQL.execute(insertQuery)
      logger.info(" rowAffected " + rowAffected)

    } catch {
      case ex: SQLIntegrityConstraintViolationException =>
        logger.info(s"Unable to upsert the record $insertQuery " + ex.getMessage)
      case ex: Exception =>
        logger.info(s"Unhandled error while storing the record $insertQuery " + ex.getMessage)
    }
    logger.info(" input obj:: " + input)
    val resp = ReportResponse(input.reportId, input.reportName.getOrElse(""), input.reportType.getOrElse(""),
      input.siteId, input.orgId, input.timePeriod.getOrElse(""),
      input.schedule.getOrElse(""), input.lastSent.getOrElse(""), input.nextScheduled.getOrElse(""),
      input.active.getOrElse(""), input.rules.getOrElse(""), input.userEmails.getOrElse(""), created_ts, 0)
    resp
  }

  def updateRecords(tableName: String, input: Report): Report = {

    val updated_ts = System.currentTimeMillis()

    val updateQuery =
      s"""
         |UPDATE  $tableName SET
         | report_name='${input.reportName.getOrElse("My Test Report")}', report_type = '${input.reportType.getOrElse("")}',
         | time_period = '${input.timePeriod.getOrElse("1day")}', schedule = '${input.schedule.getOrElse("")}',
         | active= '${input.active.getOrElse("false")}', rules= '${input.rules}', user_emails = '${input.userEmails.getOrElse("")}',
         | updated='$updated_ts'
         | where report_id ='${input.reportId}'
       """.stripMargin

    logger.info(s"update Aurora Query: $updateQuery")
    try {

      var rowAffected = mySQL.executeUpdate(updateQuery)
      logger.info(" rowAffected " + rowAffected)

    } catch {
      case ex: SQLIntegrityConstraintViolationException =>
        logger.info(s"Unable to update the record $updateQuery " + ex.getMessage)
      case ex: Exception =>
        logger.info(s"Unhandled error while updating the record $updateQuery " + ex.getMessage)
    }
    println(" input obj:: " + input)
    input.copy(reportId = input.reportId, reportName = input.reportName, reportType = input.reportType, timePeriod = input.timePeriod,
      schedule = input.schedule, lastSent = input.lastSent, nextScheduled = input.nextScheduled, active = input.active, rules = input.rules,
      userEmails = input.userEmails, created = input.created, updated = Some(updated_ts))
    //input.copy(created = Some(created_ts), updated = Some(0))
  }

  def deleteRecord(tableName: String, input: String): Boolean = {

    val deleteQuery =
      s"""
         |DELETE FROM $tableName
         | where report_id =${input}
       """.stripMargin

    logger.debug(s"Aurora Query: $deleteQuery")
    logger.info(s"Aurora Query: $deleteQuery")
    try {
      val resp = mySQL.executeUpdate(deleteQuery)
      logger.info(" resp " + resp)
      true
    } catch {
      case ex: SQLIntegrityConstraintViolationException =>
        logger.info(s"Unable to upsert the record $deleteQuery " + ex.getMessage)
        false
      case ex: Exception =>
        logger.error(s"Unhandled error while storing the record $deleteQuery " + ex.getMessage)
        logger.info(s"Unhandled error while storing the record $deleteQuery " + ex.getMessage)
        false
    }
  }

  def getAllRecords(tableName: String, primaryKey: String): List[Report] = {
    var resp = List[Report]()

    val getAllQuery =
      s"""
         |SELECT * FROM $tableName WHERE
         | site_id='${primaryKey}'
      """.stripMargin

    logger.info(s"GPS Aurora Query: $getAllQuery")
    try {
      var rs = mySQL.executeQuery(getAllQuery)

      while (rs.next) {
        logger.info(" getAllRecords result set started:: ")
        val reportName = rs.getString("report_name")
        val reportType = rs.getString("report_type")
        val timePeriod = rs.getString("time_period")
        val schedule = rs.getString("schedule")
        val lastSent = rs.getString("last_sent")
        val nextScheduled = rs.getString("next_scheduled")
        val userEmails = rs.getString("user_emails")
        val siteId = rs.getString("site_id")
        val reportId = rs.getString("report_id")
        val orgId = rs.getString("org_id")
        val active = rs.getString("active")
        val created = rs.getLong("created")
        val updated = rs.getLong("updated")

        val rules = rs.getString("rules")

        resp = Report(reportId, Option(reportName), Option(reportType), siteId, orgId,
          Option(timePeriod), Option(schedule), Option(lastSent), Option(nextScheduled), Option(active),
          Option(rules), Option(userEmails), Option(created), Option(updated)) :: resp

        logger.info(" get resp is:: " + resp)
      }
    } catch {
      case ex: SQLIntegrityConstraintViolationException =>
        logger.warn(s"Unable to upsert the record $getAllQuery " + ex.getMessage)

      case ex: Exception =>
        logger.info(s"Unhandled error while storing the record $getAllQuery " + ex.getMessage)

    }
    resp
  }

  def getRecord(tableName: String, primaryKey: String): Option[Report] = {
    var resp = None: Option[Report]
    ()
    val getQuery =
      s"""
         |SELECT * FROM $tableName WHERE
         | report_id='${primaryKey}'
      """.stripMargin

    logger.info(s"GPS Aurora Query: $getQuery")
    try {
      var rs = mySQL.executeQuery(getQuery)
      logger.info(" rs is:: " + rs)
      while (rs.next) {
        logger.info(" getRecords result set started:: ")

        val reportName = rs.getString("report_name")
        val reportType = rs.getString("report_type")
        val timePeriod = rs.getString("time_period")
        val schedule = rs.getString("schedule")
        val lastSent = rs.getString("last_sent")
        val nextScheduled = rs.getString("next_scheduled")
        val userEmails = rs.getString("user_emails")
        val siteId = rs.getString("site_id")
        val reportId = rs.getString("report_id")
        val orgId = rs.getString("org_id")

        val active = rs.getString("active")
        val created = rs.getLong("created")
        val updated = rs.getLong("updated")

        val rules = rs.getString("rules")

        resp = Some(Report(reportId, Option(reportName), Option(reportType), siteId, orgId,
          Option(timePeriod), Option(schedule), Option(lastSent), Option(nextScheduled), Option(active),
          Option(rules), Option(userEmails), Option(created), Option(updated)))

        logger.info(" get resp is:: " + resp)
      }
    } catch {
      case ex: SQLIntegrityConstraintViolationException =>
        logger.info(s"Unable to retrieving the record $getQuery " + ex.getMessage)

      case ex: Exception =>
        logger.error(s"Unhandled error while retrieving the record $getQuery " + ex.getMessage)

    }
    resp
  }
}
