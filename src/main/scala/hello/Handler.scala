package hello

import Model.{DatabaseCRUDOperations, Report, ResponseObj}
import com.amazonaws.services.lambda.runtime.events.{APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent}
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.google.gson.Gson
import org.slf4j.{Logger, LoggerFactory}

import scala.util.parsing.json.JSON

object Handler extends RequestHandler[APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent] {

  val dbConnectivityclass = new DatabaseCRUDOperations()
  implicit lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def handleRequest(input: APIGatewayProxyRequestEvent, context: Context): APIGatewayProxyResponseEvent = {
    var response = new APIGatewayProxyResponseEvent()
    val gson = new Gson

    try {
      val requestHttpMethod = input.getHttpMethod
      val requestBody = input.getBody
      val requestHeaders = input.getHeaders
      val requestPath = input.getPath
      val requestPathParameters = input.getPathParameters
      val requestQueryStringParameters = input.getQueryStringParameters

      logger.info(" input is :  " + input)
      logger.info(" requestBody " + requestBody)
      logger.info(" requestHeaders " + requestHeaders)
      logger.info(" requestHttpMethod " + requestHttpMethod)
      logger.info(" requestPath " + requestPath)
      logger.info(" requestPathParameters " + requestPathParameters)
      logger.info(" requestQueryStringParameters " + requestQueryStringParameters)

      val parsedBody = JSON.parseFull(requestBody).getOrElse(0).asInstanceOf[Map[String, String]]
      logger.info(" parsedBody is:: " + parsedBody)

      val reportName = parsedBody.get("reportName")
      val reportType = parsedBody.get("reportType")
      val timePeriod = parsedBody.get("timePeriod")
      val schedule = parsedBody.get("schedule")
      val lastSent = parsedBody.get("lastSent")
      val nextScheduled = parsedBody.get("nextScheduled")
      val rules = parsedBody.getOrElse("rules","")
      // val rulesAlertStatus = parsedBody.get("rules").getOrElse(0).asInstanceOf[Map[String, String]]
      val userEmails = parsedBody.get("userEmails")
      val active = parsedBody.getOrElse("active","false")
      val created = parsedBody.getOrElse("created","0").toLong
      val updated = parsedBody.getOrElse("updated","0").toLong


      /// Need to take from query parameters
      val siteId = parsedBody.getOrElse("siteId","")
      val reportId = parsedBody.getOrElse("reportId","")
      val orgId = parsedBody.getOrElse("orgId","")


      //val reportidQueryString = requestQueryStringParameters.getOrDefault("siteid", "")
      //val reportidQueryString = requestQueryStringParameters.getOrDefault("cust_id", "")

      /* val rulesReportObj = RulesReport(rules.get("nodeid").getOrElse(Set.empty), rules.get("nodeid").getOrElse(Set.empty),
         rulesAlertStatus.get("alertstatus").getOrElse(""), rules.get("nodeid").getOrElse(Set.empty))

    */
      var ReportTableObj = Report(reportId, Option(reportName.getOrElse("")), Option(reportType.getOrElse("")), siteId, orgId, Option(timePeriod.getOrElse("")),
        Option(schedule.getOrElse("")), Option(lastSent.getOrElse("")), Option(nextScheduled.getOrElse("")), Option(active), Option(rules), userEmails, Option(created), Option(updated))

      logger.info(" ReportTableObj is:: " + ReportTableObj)

      requestHttpMethod match {
        case "POST" =>
          logger.info(" POST Request method ")
          if (!ReportTableObj.siteId.isEmpty) {
            if (!ReportTableObj.orgId.isEmpty) {
              var hashed = hash(siteId + "@" + System.currentTimeMillis() + "." + "somesalt") //This method is called for to get the hash string
              val resp = dbConnectivityclass.insertRecords("report_table", ReportTableObj.copy(reportId = hashed))
              response.setStatusCode(200)
              response.setBody(gson.toJson(resp))
            } else {
              response.setStatusCode(400)
              response.setBody(gson.toJson(ResponseObj("Failed", "orgId field is missing.")))
            }
          } else {
            response.setStatusCode(400)
            response.setBody(gson.toJson(ResponseObj("Failed", "siteId field is missing.")))
          }
        case "PUT" =>
          //Update
          val reportidQueryString = requestQueryStringParameters.getOrDefault("reportid", "")
          logger.info(" PUT Request method ")
          val resp = dbConnectivityclass.updateRecords("report_table", ReportTableObj.copy(reportId=reportidQueryString))
          response.setStatusCode(200)
          response.setBody(gson.toJson(resp))
        case "DELETE" =>
          //Delete
          val reportidQueryString = requestQueryStringParameters.getOrDefault("reportid", "")
          logger.info(" DELETE Request method ")
          if(dbConnectivityclass.deleteRecord("report_table",reportidQueryString))
          {
            response.setStatusCode(200)
            response.setBody(gson.toJson("The Report Deleted Successfully"))
          }
        case "GET" =>
          logger.info(" GET Request method ")
          val reportidQueryString = requestQueryStringParameters.getOrDefault("reportid", "")
          if (!reportidQueryString.isEmpty) {
            val resp = dbConnectivityclass.getRecord("report", reportidQueryString)
            logger.info("getRecords resp is :: " + resp)
            response.setStatusCode(200)
            response.setBody(gson.toJson(resp))
          } else {
            val siteidQueryString = requestQueryStringParameters.get("site_id")
            val resp = dbConnectivityclass.getAllRecords("report", siteidQueryString)
            logger.info("getRecords resp is :: " + resp)
            response.setStatusCode(200)
            response.setBody(gson.toJson(resp))
          }
        case _ =>
          logger.info("Invalid Request method ")
          response.setStatusCode(500)
          response.setBody(gson.toJson("Invalid Request method entered"))
      }
    } catch {
      case e: Exception =>
        logger.info(" Exception is:: " + e.toString)
    }
    logger.info(" response is:: " + response)
    response
  }

  def hash(s: String) = {
    try {
      logger.info(" hash method initiated ")
      val m = java.security.MessageDigest.getInstance("MD5")
      val b = s.getBytes("UTF-8")
      m.update(b, 0, b.length)
      new java.math.BigInteger(1, m.digest()).toString(16)
    } catch {
      case e: Exception =>
        val errorMsg = "hash method exception:" + e.toString()
        logger.info(errorMsg)
        ""
    }
  }
}
