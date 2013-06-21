package com.twitter.algebird

import org.ejml.simple.SimpleMatrix

case class LinearRegression(xTx : SimpleMatrix, xTy : SimpleMatrix) {
  lazy val coefficients = xTx.solve(xTy)
}

object LinearRegression {
  def apply(x : Array[Array[Double]], y: Array[Double]) = {
    val xMatrix = new SimpleMatrix(x)
    val yVector = new SimpleMatrix(Array(y)).transpose
    val xTx = xMatrix.transpose.mult(xMatrix)
    val xTy = xMatrix.transpose.mult(yVector)
    new LinearRegression(xTx, xTy)
  }
}

class LinearRegressionVectorSpace(n : Int) extends Group[LinearRegression] {
//VectorSpace[Double, LinearRegression[Double]] {
  val zero = LinearRegression(new SimpleMatrix(n,n), new SimpleMatrix(n, 1))

  def plus(a : LinearRegression, b : LinearRegression) : LinearRegression = {
    val xTy = a.xTy.plus(1, b.xTy)
    val xTx = a.xTx.plus(1, b.xTx)

    LinearRegression(xTx, xTy)
  }

  def scale(beta : Double, a : LinearRegression) : LinearRegression = {
    LinearRegression(a.xTx.scale(beta), a.xTy.scale(beta))
  }
}
