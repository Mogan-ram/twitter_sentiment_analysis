//package LoggerUtil
//
//
//import org.slf4j.LoggerFactory
//import ch.qos.logback.classic.LoggerContext
//import ch.qos.logback.classic.joran.JoranConfigurator
//import ch.qos.logback.core.util.StatusPrinter
//import java.io.File
//
//object LoggerUtil {
//  private val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
//  private val configFile = new File("src/main/resources/logback.xml")
//
//  // Initialize Logback with file configuration
//  private def initializeLogging(): Unit = {
//    if (!configFile.exists()) {
//      val defaultConfig =
//        """<configuration>
//          |  <appender name="FILE" class="ch.qos.logback.core.FileAppender">
//          |    <file>reddit-sentiment-logs.log</file>
//          |    <encoder>
//          |      <pattern>%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n</pattern>
//          |    </encoder>
//          |  </appender>
//          |  <root level="info">
//          |    <appender-ref ref="FILE" />
//          |  </root>
//          |</configuration>""".stripMargin
//
//      val configurator = new JoranConfigurator()
//      configurator.setContext(loggerContext)
//      loggerContext.reset()
//      configurator.doConfigure(new java.io.StringReader(defaultConfig)) // Use StringReader for XML string
//      StatusPrinter.printInCaseOfErrorsOrWarnings(loggerContext)
//    }
//  }
//
//  initializeLogging()
//
//  def getLogger(className: Class[_]): org.slf4j.Logger = {
//    LoggerFactory.getLogger(className)
//  }
//}