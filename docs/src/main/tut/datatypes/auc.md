---
layout: docs
title:  "Binary Classifier AUC"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/BinaryClassificationAUC.scala"
scaladoc: "#com.twitter.algebird.BinaryClassificationAUC"
---

# Binary Classifier AUC

One of the common metrics to check, especially for binary classification problems, is the Area Under the Curve(AUC).

For a general overview of this metric for the ROC case here - [Curves in ROC Space](https://en.wikipedia.org/wiki/Receiver_operating_characteristic#Curves_in_ROC_space)

## Predictions

The starting point for this metric is a binary prediction. This is a binary prediction given by a classifier or some other
algorithm.

```tut:book
import com.twitter.algebird._

val prediction = BinaryPrediction(0.5, true)
```

The first option is the predicted score while the second is the label.

## Confusion Matrix

Given a set of predictions a Confusion Matrix can be built based on a given threshold using
the Aggregator.  On the present function call this will also compute some common metrics like Precision,
Recall, F-Score, and False Positive Rate.

```tut:book
import com.twitter.algebird._

val predictions = List(BinaryPrediction(0.9, true), BinaryPrediction(0.1, false))

val scored = BinaryClassificationConfusionMatrixAggregator()(predictions)
```

## Area Under the Curve

A curve can be defined by a sequence of Confusion Matrices under various thresholds. The BinaryClassificationAUCAggregator
does this for you given a number of samples.

```tut:book
import com.twitter.algebird._

val predictions = List(BinaryPrediction(0.9, true), BinaryPrediction(0.1, false))

// ROC AUC
val roc = BinaryClassificationAUCAggregator(ROC)(predictions)

// PR AUC
val auc = BinaryClassificationAUCAggregator(PR)(predictions)
```
