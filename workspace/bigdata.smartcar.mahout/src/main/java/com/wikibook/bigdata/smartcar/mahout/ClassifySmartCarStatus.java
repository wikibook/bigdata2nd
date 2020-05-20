package com.wikibook.bigdata.smartcar.mahout;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.classifier.df.DecisionForest;
import org.apache.mahout.classifier.df.data.Dataset;
import org.apache.mahout.classifier.df.data.Instance;

import org.apache.mahout.math.DenseVector;

public class ClassifySmartCarStatus {

  public static void main(String[] args) throws IOException {
    Configuration config = new Configuration();

    DecisionForest classifyModel = DecisionForest.load(
    		config, new Path("./classification/model/forest.seq"));
    Dataset dataset = Dataset.load(
    		config, new Path("./classification/descriptor/smartcar-status.descriptor"));
    
    int i=0;
    
    DenseVector car_vector = new DenseVector(14);
    car_vector.set(0, Integer.parseInt(args[i++])); 	//car_capacity
    car_vector.set(1, Integer.parseInt(args[i++])); 	//car_year
    car_vector.set(2, dataset.valueOf(2, args[i++])); 	//car_model
    car_vector.set(3, Integer.parseInt(args[i++])); 	//타이어 1
    car_vector.set(4, Integer.parseInt(args[i++])); 	//타이어 2
    car_vector.set(5, Integer.parseInt(args[i++])); 	//타이어 3
    car_vector.set(6, Integer.parseInt(args[i++])); 	//타이어 4
    car_vector.set(7, Integer.parseInt(args[i++]));  	//라이트 1
    car_vector.set(8, Integer.parseInt(args[i++]));  	//라이트 2
    car_vector.set(9, Integer.parseInt(args[i++]));  	//라이트 3
    car_vector.set(10, Integer.parseInt(args[i++])); 	//라이트 4
    car_vector.set(11, dataset.valueOf(11, args[i++])); //엔진
    car_vector.set(12, dataset.valueOf(12, args[i++])); //브레이크
    car_vector.set(13, Integer.parseInt(args[i++])); 	//베터리

    Instance instance = new Instance(car_vector);
    Random rNum = RandomUtils.getRandom();

    double prediction = classifyModel.classify(dataset, rNum, instance);

    System.out.println(" SmartCar Status Prediction :"+dataset.getLabelString(prediction));
  }
}












