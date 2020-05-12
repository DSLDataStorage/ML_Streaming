/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ds_join

import scala.reflect.ClassTag

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithm
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.StreamingLinearAlgorithm
//import org.apache.spark.mllib.regression._

/**
 * Train or predict a linear regression model on streaming data. Training uses
 * Stochastic Gradient Descent to update the model based on each new batch of
 * incoming data from a DStream (see `LinearRegressionWithSGD` for model equation)
 *
 * Each batch of data is assumed to be an RDD of LabeledPoints.
 * The number of data points per batch can vary, but the number
 * of features must be constant. An initial weight
 * vector must be provided.
 *
 * Use a builder pattern to construct a streaming linear regression
 * analysis in an application, like:
 *
 *  val model = new StreamingLinearRegressionWithSGD()
 *    .setStepSize(0.5)
 *    .setNumIterations(10)
 *    .setInitialWeights(Vectors.dense(...))
 *    .trainOn(DStream)

class LinearRegressionWithSGD(
    private var stepSize: Double,
    private var numIterations: Int,
    private var regParam: Double,
    private var miniBatchFraction: Double)
  extends GeneralizedLinearAlgorithm[LinearRegressionModel] with Serializable {

  private val gradient = new LeastSquaresGradient()
  private val updater = new SimpleUpdater()
\
  override val optimizer = new GradientDescent(gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setRegParam(regParam)
    .setMiniBatchFraction(miniBatchFraction)

  /**
   * Construct a LinearRegression object with default parameters: {stepSize: 1.0,
   * numIterations: 100, miniBatchFraction: 1.0}.
   */
 
  //@deprecated("Use ml.regression.LinearRegression or LBFGS", "2.0.0")
  def this() = this(1.0, 100, 0.0, 1.0)

  override def createModel(weights: Vector, intercept: Double) = {
    new LinearRegressionModel(weights, intercept)
  }
}

 */


class StreamingLinearRegressionWithSGD_dsl (
    private var stepSize: Double,
    private var numIterations: Int,
    private var regParam: Double,
    private var miniBatchFraction: Double)
  extends StreamingLinearAlgorithm[LinearRegressionModel, LinearRegressionWithSGD]
  with Serializable {

  /**
   * Construct a StreamingLinearRegression object with default parameters:
   * {stepSize: 0.1, numIterations: 50, miniBatchFraction: 1.0}.
   * Initial weights must be set before using trainOn or predictOn
   * (see `StreamingLinearAlgorithm`)
   */

  def this() = this(0.1, 50, 0.0, 1.0)


  val algorithm = new LinearRegressionWithSGD()

  protected var model: Option[LinearRegressionModel] = None

  /**
   * Set the step size for gradient descent. Default: 0.1.
   */

  def setStepSize(stepSize: Double): this.type = {
    this.algorithm.optimizer.setStepSize(stepSize)
    this
  }

  /**
   * Set the regularization parameter. Default: 0.0.
   */

  def setRegParam(regParam: Double): this.type = {
    this.algorithm.optimizer.setRegParam(regParam)
    this
  }

  /**
   * Set the number of iterations of gradient descent to run per update. Default: 50.
   */

  def setNumIterations(numIterations: Int): this.type = {
    this.algorithm.optimizer.setNumIterations(numIterations)
    this
  }

  /**
   * Set the fraction of each batch to use for updates. Default: 1.0.
   */

  def setMiniBatchFraction(miniBatchFraction: Double): this.type = {
    this.algorithm.optimizer.setMiniBatchFraction(miniBatchFraction)
    this
  }

  /**
   * Set the initial weights.
   */

  def setInitialWeights(initialWeights: Vector): this.type = {
    this.model = Some(new LinearRegressionModel(initialWeights, 0.0))  // new LinearRegressionModel(initialWeights, 0.0)
    this
  }

  /**
   * Set the convergence tolerance. Default: 0.001.
   */

  def setConvergenceTol(tolerance: Double): this.type = {
    this.algorithm.optimizer.setConvergenceTol(tolerance)
    this
  }

  def trainOn_dsl(data: org.apache.spark.rdd.RDD[LabeledPoint]): Unit = {
    if (model.isEmpty) {
      throw new IllegalArgumentException("Model must be initialized before starting training.")
    }

        model = Some(algorithm.run(data, latestModel().weights))
        val display = latestModel().weights.size match {
          case x if x > 100 => latestModel().weights.toArray.take(100).mkString("[", ",", "...")
          case _ => latestModel().weights.toArray.mkString("[", ",", "]")
        }


  }


  def predictOnValues_dsl(data: org.apache.spark.rdd.RDD[(Double, Vector)]): org.apache.spark.rdd.RDD[(Double, Double)] = {
    if (model.isEmpty) {
      throw new IllegalArgumentException("Model must be initialized before starting prediction")
    }
    data.mapValues{x => latestModel().predict(x)}
  }



}