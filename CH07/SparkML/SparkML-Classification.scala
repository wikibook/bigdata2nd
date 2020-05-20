//그림 7.62 스파크ML의 라이브러리 임포트------------------------------------------------------

import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.evaluation.MulticlassMetrics 
import org.apache.spark.mllib.util.MLUtils

//그림 7.63 스파크ML의 학습 데이터 로드------------------------------------------------------

val ds = spark.read.csv("/pilot-pjt/spark-data/classification/input/classification_dataset.txt")
ds.show(5)

//그림 7.64 스파크ML에서 사용할 칼럼 선택------------------------------------------------------

val dsSmartCar = ds.selectExpr("cast(_c5 as long) car_capacity", 
                        "cast(_c6 as long) car_year",
                        "cast(_c7 as string) car_model",
                        "cast(_c8 as int) tire_fl",
                        "cast(_c9 as long) tire_fr",
                        "cast(_c10 as long) tire_bl",
                        "cast(_c11 as long) tire_br",
                        "cast(_c12 as long) light_fl",
                        "cast(_c13 as long) light_fr",
                        "cast(_c14 as long) light_bl",
                        "cast(_c15 as long) light_br",
                        "cast(_c16 as string) engine",
                        "cast(_c17 as string) break",
                        "cast(_c18 as long) battery",
                        "cast(_c19 as string) status"
                       )


//그림 7.65 범주형 칼럼을 연속형(숫자형) 칼럼으로 변환 및 생성------------------------------------------------------

val dsSmartCar_1 = new StringIndexer().setInputCol("car_model").setOutputCol("car_model_n").fit(dsSmartCar).transform(dsSmartCar)
val dsSmartCar_2 = new StringIndexer().setInputCol("engine").setOutputCol("engine_n").fit(dsSmartCar_1).transform(dsSmartCar_1)
val dsSmartCar_3 = new StringIndexer().setInputCol("break").setOutputCol("break_n").fit(dsSmartCar_2).transform(dsSmartCar_2)
val dsSmartCar_4 = new StringIndexer().setInputCol("status").setOutputCol("label").fit(dsSmartCar_3).transform(dsSmartCar_3)
val dsSmartCar_5 = dsSmartCar_4.drop("car_model").drop("engine").drop("break").drop("status")

dsSmartCar_5.show()


//그림 7.66 스파크ML에 사용할 피처 변수 작업------------------------------------------------------

val cols = Array("car_capacity", "car_year", "car_model_n", "tire_fl",
                 "tire_fr", "tire_bl", "tire_br", "light_fl", "light_fr", 
                 "light_bl", "light_br", "engine_n", "break_n", "battery")

val dsSmartCar_6 = new VectorAssembler().setInputCols(cols).setOutputCol("features").transform(dsSmartCar_5)
val dsSmartCar_7 = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures").fit(dsSmartCar_6).transform(dsSmartCar_6)
val dsSmartCar_8 = dsSmartCar_7.drop("features").withColumnRenamed("scaledfeatures", "features")
dsSmartCar_8.show()

//그림 7.67 머신러닝 학습용 데이터를 LibSVM 형식으로 저장------------------------------------------------------

val dsSmartCar_9 = dsSmartCar_8.select("label", "features")
dsSmartCar_9.write.format("libsvm").save("/pilot-pjt/spark-data/classification/smartCarLibSVM")

//그림 7.69 LibSVM 형식의 머신러닝 학습용 데이터 확인 및 로드------------------------------------------------------

val dsSmartCar_10 = spark.read.format("libsvm").load("/pilot-pjt/spark-data/classification/smartCarLibSVM")
dsSmartCar_10.show(5)

//그림 7.70 Training 및 Test 데이터셋 생성------------------------------------------------------

val labelIndexer = new StringIndexer()
  .setInputCol("label")
  .setOutputCol("indexedLabel")
  .fit(dsSmartCar_10)

val featureIndexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexedFeatures")
  .fit(dsSmartCar_10)

val Array(trainingData, testData) = dsSmartCar_10.randomSplit(Array(0.7, 0.3))

//그림 7.71 스마트카의 상태 정보 예측을 위한 랜덤 포레스트 모델 학습------------------------------------------------------

val rf = new RandomForestClassifier()
  .setLabelCol("indexedLabel")
  .setFeaturesCol("indexedFeatures")
  .setNumTrees(5)
  
  
val labelConverter = new IndexToString()
  .setInputCol("prediction")
  .setOutputCol("predictedLabel")
  .setLabels(labelIndexer.labels)
  
  
val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))


val model = pipeline.fit(trainingData)

//그림 7.72 랜덤 포레스트 모델의 설명력 확인------------------------------------------------------


val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
println(s"RandomForest Model Description :\n ${rfModel.toDebugString}")

//그림 7.73 랜덤 포레스트 모델 평가기 실행------------------------------------------------------



val predictions = model.transform(testData)
predictions.select("predictedLabel", "label", "features").show(5)

val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("indexedLabel")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")
val accuracy = evaluator.evaluate(predictions)


//그림 7.74 스마트카 상태 예측 모델 평가 – 정확도------------------------------------------------------


println(s"@ Accuracy Rate = ${(accuracy)}")
println(s"@ Error Rate = ${(1.0 - accuracy)}")


//그림 7.75 스마트카 상태 예측 모델 평가 – Confusion Matrix 실행------------------------------------------------------

val results = model.transform(testData).select("features", "label", "prediction")
val predictionAndLabels = results.select($"prediction",$"label").as[(Double, Double)].rdd

val bMetrics = new BinaryClassificationMetrics(predictionAndLabels)
val mMetrics = new MulticlassMetrics(predictionAndLabels)
val labels = mMetrics.labels

println("Confusion Matrix:")
println(mMetrics.confusionMatrix)

//그림 7.77 스마트카 상태 예측 모델 평가 – Precision(정밀도)------------------------------------------------------

 labels.foreach { rate =>
    println(s"@ Precision Rate($rate) = " + mMetrics.precision(rate))
 }

//그림 7.78 스마트카 상태 예측 모델 평가 – Recall(재현율)------------------------------------------------------
 labels.foreach { rate =>
    println(s"Recall Rate($rate) = " + mMetrics.recall(rate))
 }
 labels.foreach { rate =>
   println(s"False Positive Rate($rate) = " + mMetrics.falsePositiveRate(rate))
 }

//그림 7.79 스마트카 상태 예측 모델 평가 – F1-Score------------------------------------------------------
 labels.foreach { rate =>
   println(s"F1-Score($rate) = " + mMetrics.fMeasure(rate))
 }