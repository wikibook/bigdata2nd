//그림 7.96 스파크ML 라이브러리 임포트--------------------------------------------------------------

import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator

//그림 7.97 스마트카 마스터 데이터셋 로드--------------------------------------------------------------

val ds = spark.read.option("delimiter", " ")
                   .csv("/pilot-pjt/mahout/clustering/input/smartcar_master.txt")
ds.show(5)

//그림 7.98 스마트카 마스터 데이터셋 재구성--------------------------------------------------------------

val dsSmartCar_Master_1 = ds.selectExpr(
                        "cast(_c0 as string) car_number",
                        "cast(_c1 as string) car_capacity",
                        "cast(_c2 as string) car_year",
                        "cast(_c3 as string) car_model",
                        "cast(_c4 as string) sex",
                        "cast(_c5 as string) age",
                        "cast(_c6 as string) marriage",
                        "cast(_c7 as string) job",
                        "cast(_c8 as string) region"
                       )

//그림 7.99 문자형 칼럼을 연속형(숫자형) 칼럼으로 변환 및 생성--------------------------------------------------------------


val dsSmartCar_Master_2 = new StringIndexer().setInputCol("car_capacity").setOutputCol("car_capacity_n")
                                             .fit(dsSmartCar_Master_1).transform(dsSmartCar_Master_1)
val dsSmartCar_Master_3 = new StringIndexer().setInputCol("car_year").setOutputCol("car_year_n")
                                             .fit(dsSmartCar_Master_2).transform(dsSmartCar_Master_2)
val dsSmartCar_Master_4 = new StringIndexer().setInputCol("car_model").setOutputCol("car_model_n")
                                             .fit(dsSmartCar_Master_3).transform(dsSmartCar_Master_3)
val dsSmartCar_Master_5 = new StringIndexer().setInputCol("sex").setOutputCol("sex_n")
                                             .fit(dsSmartCar_Master_4).transform(dsSmartCar_Master_4)
val dsSmartCar_Master_6 = new StringIndexer().setInputCol("age").setOutputCol("age_n")
                                             .fit(dsSmartCar_Master_5).transform(dsSmartCar_Master_5)
val dsSmartCar_Master_7 = new StringIndexer().setInputCol("marriage").setOutputCol("marriage_n")
                                             .fit(dsSmartCar_Master_6).transform(dsSmartCar_Master_6)
val dsSmartCar_Master_8 = new StringIndexer().setInputCol("job").setOutputCol("job_n")
                                             .fit(dsSmartCar_Master_7).transform(dsSmartCar_Master_7)
val dsSmartCar_Master_9 = new StringIndexer().setInputCol("region").setOutputCol("region_n")
                                             .fit(dsSmartCar_Master_8).transform(dsSmartCar_Master_8)


//그림 7.100 클러스터링에 사용할 피처 변수 선택--------------------------------------------------------------

val cols = Array("car_capacity_n", "car_year_n", "car_model_n", "sex_n", "marriage_n")

val dsSmartCar_Master_10 = new VectorAssembler().setInputCols(cols).setOutputCol("features")
                                                .transform(dsSmartCar_Master_9)
val dsSmartCar_Master_11 = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
                                             .fit(dsSmartCar_Master_10).transform(dsSmartCar_Master_10)

//그림 7.101 스마트카 마스터 데이터셋 확인 및 학습/검증 데이터 생성----------------------------------------------


val dsSmartCar_Master_12 = dsSmartCar_Master_11.drop("car_capacity").drop("car_year").drop("car_model").drop("sex")
                                               .drop("age").drop("marriage").drop("job").drop("region").drop("features")
                                               .withColumnRenamed("scaledfeatures", "features")
dsSmartCar_Master_12.show(5)                                               

val Array(trainingData, testData) = dsSmartCar_Master_12.randomSplit(Array(0.7, 0.3))

//그림 7.102 스파크ML에서의 K-Means 군집 분석 실행--------------------------------------------------------------


val kmeans = new KMeans()
  .setSeed(1L)
  .setK(200)
  .setFeaturesCol("features")
  .setPredictionCol("prediction")
val kmeansModel = kmeans.fit(dsSmartCar_Master_12)


//그림 7.103 스파크ML에서의 K-Means 군집 결과 확인--------------------------------------------------------------


val transKmeansModel = kmeansModel.transform(dsSmartCar_Master_12)
transKmeansModel.groupBy("prediction").agg(collect_set("car_number").as("car_number")).orderBy("prediction").show(200, false)


//그림 7.104 스파크ML에서의 K-Means 군집 모델 평가 – 실루엣 스코어--------------------------------------------------------------

val evaluator = new ClusteringEvaluator()
val silhouette = evaluator.evaluate(transKmeansModel)

println(s"Silhouette Score = $silhouette")