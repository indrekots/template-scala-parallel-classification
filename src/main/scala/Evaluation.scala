package org.template.classification

import io.prediction.controller.AverageMetric
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EngineParams
import io.prediction.controller.EngineParamsGenerator
import io.prediction.controller.Evaluation

case class Accuracy()
  extends AverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {
  def calculate(query: Query, predicted: PredictedResult, actual: ActualResult)
  : Double = (if (predicted.label == actual.label) 1.0 else 0.0)
}

object AccuracyEvaluation extends Evaluation {
  // Define Engine and Metric used in Evaluation
  engineMetric = (ClassificationEngine(), new Accuracy())
}

object EngineParamsList extends EngineParamsGenerator {
  // Define list of EngineParams used in Evaluation

  // First, we define the base engine params. It specifies the appId from which
  // the data is read, and a evalK parameter is used to define the
  // cross-validation.
  private[this] val baseEP = EngineParams(
    dataSourceParams = DataSourceParams(appName = "Class2", evalK = Some(5)))

  // Second, we specify the engine params list by explicitly listing all
  // algorithm parameters. In this case, we evaluate 3 engine params, each with
  // a different algorithm params value.
  engineParamsList = Seq(
//    baseEP.copy(algorithmParamsList = Seq(("naive", AlgorithmParams(0.1)))),
//    baseEP.copy(algorithmParamsList = Seq(("naive", AlgorithmParams(1.0)))),
//    baseEP.copy(algorithmParamsList = Seq(("naive", AlgorithmParams(10.0)))),
//    baseEP.copy(algorithmParamsList = Seq(("naive", AlgorithmParams(100.0)))),
//    baseEP.copy(algorithmParamsList = Seq(("naive", AlgorithmParams(1000.0)))),
//    baseEP.copy(algorithmParamsList = Seq(("naive", AlgorithmParams(2000.0)))),
//    baseEP.copy(algorithmParamsList = Seq(("naive", AlgorithmParams(10000.0)))),
//    baseEP.copy(algorithmParamsList = Seq(("naive", AlgorithmParams(20000.0)))),
//    baseEP.copy(algorithmParamsList = Seq(("naive", AlgorithmParams(100000.0)))),
//    baseEP.copy(algorithmParamsList = Seq(("randomforest", RandomForestAlgorithmParams(2, 25, "auto", "gini", 11, 100)))),
//    baseEP.copy(algorithmParamsList = Seq(("randomforest", RandomForestAlgorithmParams(2, 27, "auto", "gini", 11, 100)))),
//    baseEP.copy(algorithmParamsList = Seq(("randomforest", RandomForestAlgorithmParams(2, 30, "auto", "gini", 11, 100)))),
//    baseEP.copy(algorithmParamsList = Seq(("randomforest", RandomForestAlgorithmParams(2, 40, "auto", "gini", 20, 100)))),
    baseEP.copy(algorithmParamsList = Seq(("randomforest", RandomForestAlgorithmParams(2, 50, "auto", "gini", 20, 50)))),
    baseEP.copy(algorithmParamsList = Seq(("randomforest", RandomForestAlgorithmParams(2, 50, "auto", "gini", 20, 100))))
  )
}
