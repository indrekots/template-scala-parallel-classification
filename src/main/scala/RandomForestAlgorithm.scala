package org.template.classification

import io.prediction.controller.P2LAlgorithm
import io.prediction.controller.Params

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext

import grizzled.slf4j.Logger

case class RandomForestAlgorithmParams(
  numClasses: Int,
  numTrees: Int,
  featureSubsetStrategy: String,
  impurity: String,
  maxDepth: Int,
  maxBins: Int
) extends Params

// extends P2LAlgorithm because the MLlib's NaiveBayesModel doesn't contain RDD.
class RandomForestAlgorithm(val ap: RandomForestAlgorithmParams)
  extends P2LAlgorithm[PreparedData, RandomForestModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): RandomForestModel = {
    // CHANGED
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val categoricalFeaturesInfo = Map[Int, Int]()
    RandomForest.trainClassifier(
      data.labeledPoints,
      ap.numClasses,
      categoricalFeaturesInfo,
      ap.numTrees,
      ap.featureSubsetStrategy,
      ap.impurity,
      ap.maxDepth,
      ap.maxBins)
  }

  def predict(model: RandomForestModel, query: Query): PredictedResult = {
    val label = model.predict(Vectors.dense(
      Array(query.attr0, query.attr1, query.attr2, query.attr3, query.attr4, query.attr5, query.attr6,
            query.attr7, query.attr8, query.attr9, query.attr10, query.attr11, query.attr12, query.attr13,
            query.attr14, query.attr15)
    ))
    new PredictedResult(label)
  }

}
