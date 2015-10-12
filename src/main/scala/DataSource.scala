package org.template.classification

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.Params
import io.prediction.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import grizzled.slf4j.Logger

case class DataSourceParams(
  appName: String,
  evalK: Option[Int]  // define the k-fold parameter.
) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, ActualResult] {

  @transient lazy val logger = Logger[this.type]
  val requiredElements = List("plan", "attr0", "attr1", "attr2",
                      "attr3", "attr4", "attr5", "attr6", "attr7",
                      "attr8", "attr9", "attr10", "attr11", "attr12",
                      "attr13", "attr14", "attr15", "attr16", "attr17",
                      "attr18", "attr19", "attr20", "attr21", "attr22",
                      "attr23", "attr24", "attr25", "attr26", "attr27",
                      "attr28", "attr29", "attr30", "attr31", "attr32",
                      "attr33", "attr34", "attr35")

  override
  def readTraining(sc: SparkContext): TrainingData = {

    val labeledPoints: RDD[LabeledPoint] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user",
      // only keep entities with these required properties defined
      required = Some(requiredElements))(sc)
      // aggregateProperties() returns RDD pair of
      // entity ID and its aggregated properties
      .map { case (entityId, properties) =>
        try {
          LabeledPoint(properties.get[Double]("plan"),
            Vectors.dense(Array(
              properties.get[Double]("attr0"),
              properties.get[Double]("attr1"),
              properties.get[Double]("attr2"),
              properties.get[Double]("attr3"),
              properties.get[Double]("attr4"),
              properties.get[Double]("attr5"),
              properties.get[Double]("attr6"),
              properties.get[Double]("attr7"),
              properties.get[Double]("attr8"),
              properties.get[Double]("attr9"),
              properties.get[Double]("attr10"),
              properties.get[Double]("attr11"),
              properties.get[Double]("attr12"),
              properties.get[Double]("attr13"),
              properties.get[Double]("attr14"),
              properties.get[Double]("attr15"),
              properties.get[Double]("attr16"),
              properties.get[Double]("attr17"),
              properties.get[Double]("attr18"),
              properties.get[Double]("attr19"),
              properties.get[Double]("attr20"),
              properties.get[Double]("attr21"),
              properties.get[Double]("attr22"),
              properties.get[Double]("attr23"),
              properties.get[Double]("attr24"),
              properties.get[Double]("attr25"),
              properties.get[Double]("attr26"),
              properties.get[Double]("attr27"),
              properties.get[Double]("attr28"),
              properties.get[Double]("attr29"),
              properties.get[Double]("attr30"),
              properties.get[Double]("attr31"),
              properties.get[Double]("attr32"),
              properties.get[Double]("attr33"),
              properties.get[Double]("attr34"),
              properties.get[Double]("attr35")
            ))
          )
        } catch {
          case e: Exception => {
            logger.error(s"Failed to get properties ${properties} of" +
              s" ${entityId}. Exception: ${e}.")
            throw e
          }
        }
      }.cache()

    new TrainingData(labeledPoints)
  }

  override
  def readEval(sc: SparkContext)
  : Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, ActualResult)])] = {
    require(dsp.evalK.nonEmpty, "DataSourceParams.evalK must not be None")

    // The following code reads the data from data store. It is equivalent to
    // the readTraining method. We copy-and-paste the exact code here for
    // illustration purpose, a recommended approach is to factor out this logic
    // into a helper function and have both readTraining and readEval call the
    // helper.
    val labeledPoints: RDD[LabeledPoint] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user",
      // only keep entities with these required properties defined
      required = Some(requiredElements))(sc)
      // aggregateProperties() returns RDD pair of
      // entity ID and its aggregated properties
      .map { case (entityId, properties) =>
        try {
          LabeledPoint(properties.get[Double]("plan"),
            Vectors.dense(Array(
              properties.get[Double]("attr0"),
              properties.get[Double]("attr1"),
              properties.get[Double]("attr2"),
              properties.get[Double]("attr3"),
              properties.get[Double]("attr4"),
              properties.get[Double]("attr5"),
              properties.get[Double]("attr6"),
              properties.get[Double]("attr7"),
              properties.get[Double]("attr8"),
              properties.get[Double]("attr9"),
              properties.get[Double]("attr10"),
              properties.get[Double]("attr11"),
              properties.get[Double]("attr12"),
              properties.get[Double]("attr13"),
              properties.get[Double]("attr14"),
              properties.get[Double]("attr15"),
              properties.get[Double]("attr16"),
              properties.get[Double]("attr17"),
              properties.get[Double]("attr18"),
              properties.get[Double]("attr19"),
              properties.get[Double]("attr20"),
              properties.get[Double]("attr21"),
              properties.get[Double]("attr22"),
              properties.get[Double]("attr23"),
              properties.get[Double]("attr24"),
              properties.get[Double]("attr25"),
              properties.get[Double]("attr26"),
              properties.get[Double]("attr27"),
              properties.get[Double]("attr28"),
              properties.get[Double]("attr29"),
              properties.get[Double]("attr30"),
              properties.get[Double]("attr31"),
              properties.get[Double]("attr32"),
              properties.get[Double]("attr33"),
              properties.get[Double]("attr34"),
              properties.get[Double]("attr35")
            ))
          )
        } catch {
          case e: Exception => {
            logger.error(s"Failed to get properties ${properties} of" +
              s" ${entityId}. Exception: ${e}.")
            throw e
          }
        }
      }.cache()
    // End of reading from data store

    // K-fold splitting
    val evalK = dsp.evalK.get
    val indexedPoints: RDD[(LabeledPoint, Long)] = labeledPoints.zipWithIndex()

    (0 until evalK).map { idx =>
      val trainingPoints = indexedPoints.filter(_._2 % evalK != idx).map(_._1)
      val testingPoints = indexedPoints.filter(_._2 % evalK == idx).map(_._1)

      (
        new TrainingData(trainingPoints),
        new EmptyEvaluationInfo(),
        testingPoints.map {
          p => (new Query(p.features.toArray), new ActualResult(p.label))
        }
      )
    }
  }
}

class TrainingData(
  val labeledPoints: RDD[LabeledPoint]
) extends Serializable
