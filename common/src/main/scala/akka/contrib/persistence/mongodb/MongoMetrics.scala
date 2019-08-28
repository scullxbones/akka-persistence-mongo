package akka.contrib.persistence.mongodb

import com.codahale.metrics.Timer.Context
import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import nl.grons.metrics4.scala._

/**
  * Builds timers and histograms to record metrics.
  * This class uses either the [[MetricsBuilder]] specified by [[MongoSettings.MongoMetricsBuilderClass]] or if none
  * is specified [[DropwizardMetrics]] will be used.
  */
trait MongoMetrics extends MetricsBuilder with BaseBuilder {

  def driver: MongoPersistenceDriver

  /**
    * Builds a timer with the given name appended to [[BaseBuilder.metricBaseName]]
    *
    * @param name The name of the timer. It will get appended to [[BaseBuilder.metricBaseName]]
    * @return the timer.
    */
  override def timer(name: String): MongoTimer = metrics.timer(metricBaseName.append(name).name)

  /**
    * Builds a histogram with the given name appended to [[BaseBuilder.metricBaseName]]
    *
    * @param name The name of the histogram. It will get appended to [[BaseBuilder.metricBaseName]]
    * @return the histogram.
    */
  override def histogram(name: String): MongoHistogram = metrics.histogram(metricBaseName.append(name).name)

  private[this] lazy val metrics: MetricsBuilder = {
    val mongoMetricsBuilderClass: String = driver.settings.MongoMetricsBuilderClass.trim
    if (mongoMetricsBuilderClass.nonEmpty) {
      val reflectiveAccess = ReflectiveLookupExtension(driver.actorSystem)
      val builderClass = reflectiveAccess.unsafeReflectClassByName[MetricsBuilder](mongoMetricsBuilderClass)
      val builderCons = builderClass.getConstructor()
      builderCons.newInstance()
    } else {
      DropwizardMetrics
    }
  }
}

trait MongoTimer {
  /**
    * Starts a timer.
    *
    * @return the started timer.
    */
  def start(): StartedMongoTimer
}

trait StartedMongoTimer {
  /**
    * Stops the timer.
    *
    * @return the measured time in nano seconds.
    */
  def stop(): Long
}

trait MongoHistogram {

  /**
    * Records the specified value in the histogram.
    *
    * @param value The value to record.
    * @return This histogram.
    */
  def record(value: Int): Unit
}

trait MetricsBuilder {

  /**
    * Builds a timer with the given name.
    *
    * @param name The name of the timer.
    * @return the timer.
    */
  def timer(name: String): MongoTimer

  /**
    * Builds a histogram with the given name.
    *
    * @param name The name of the histogram.
    * @return the histogram.
    */
  def histogram(name: String): MongoHistogram
}

private class DropwizardTimer(dropwizardTimer: Timer) extends MongoTimer {

  /**
    * Starts a timer.
    *
    * @return the started timer.
    */
  override def start(): StartedMongoTimer = new StartedDropwizardTimer(dropwizardTimer.timerContext())
}

private class StartedDropwizardTimer(timerContext: Context) extends StartedMongoTimer {
  /**
    * Stops the timer.
    *
    * @return the measured time in nano seconds.
    */
  override def stop(): Long = timerContext.stop()
}

private class DropwizardHistogram(dropwizardHistogram: Histogram) extends MongoHistogram {

  /**
    * Records the specified value in the histogram.
    *
    * @param value The value to record.
    * @return This histogram.
    */
  override def record(value: Int): Unit = {
    dropwizardHistogram.+=(value)
  }
}

private[mongodb] object DropwizardMetrics extends MetricsBuilder with InstrumentedBuilder {

  override lazy val metricBaseName: MetricName = MetricName("")
  override lazy val metricRegistry: MetricRegistry = SharedMetricRegistries.getOrCreate("mongodb")

  private def timerName(metric: String) = MetricName(metric, "timer").name

  private def histName(metric: String) = MetricName(metric, "histo").name

  override def timer(name: String): MongoTimer = {
    new DropwizardTimer(metrics.timer(timerName(name)))
  }

  override def histogram(name: String): MongoHistogram = {
    new DropwizardHistogram(metrics.histogram(histName(name)))
  }
}
