###그림 8.21 라이브러리 임포트 및 학습 데이터 로드##################################################################

import tensorflow as tf
import pandas as pd
import matplotlib.pyplot as plt

from time import time
from tensorflow.python.keras.callbacks import TensorBoard
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_curve, roc_auc_score, auc
from keras.utils import to_categorical

df = pd.read_csv('C:/training_data/CarDrivingIncidentInfo.csv')

###그림 8.22 학습 데이터 전처리##################################################################

X = df.iloc[:, :-1].values
Y = df.iloc[:, -1].values

X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=1)

Y_train = to_categorical(Y_train)
Y_test = to_categorical(Y_test)

sc = StandardScaler()
X_train = sc.fit_transform(X_train)
X_test = sc.transform(X_test)

###그림 8.23 DNN 모델 구성 및 요약 정보 출력##################################################################

model = Sequential([
    Dense(10, input_dim=10, activation='relu'),
    Dense(20, activation='relu'),
    Dropout(0.25),
    Dense(10, activation='relu'),
    Dense(3, activation='softmax')
])
model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
model.summary()

###그림 8.25 DNN 모델 학습##################################################################

tensorboard = keras.callbacks.TensorBoard(log_dir='./graph', histogram_freq=1, write_graph=True, write_images=True)

hist  = model.fit(X_train, Y_train, batch_size=2000, epochs=50, callbacks=[tensorboard], validation_data=(X_test, Y_test))

###그림 8.26 DNN 모델을 학습한 결과##################################################################

score = model.evaluate(X_test, Y_test, verbose=0)
print(model.metrics_names)
print(score)

###그림 8.27 DNN 모델의 학습 결과 시각화##################################################################

fig, loss_ax = plt.subplots()
acc_ax = loss_ax.twinx()

loss_ax.plot(hist.history['loss'], 'y', label='train loss')
loss_ax.plot(hist.history['val_loss'], 'r', label='val loss')
acc_ax.plot(hist.history['acc'], 'b', label='train acc')
acc_ax.plot(hist.history['val_acc'], 'g', label='val acc')

loss_ax.set_xlabel('epoch')

loss_ax.set_ylabel('loss')
loss_ax.legend(loc='lower right')

acc_ax.set_ylabel('accuracy')
acc_ax.legend(loc='upper right')

plt.show()

###그림 8.28 DNN 모델 예측 결과 평가 – ROC 커브##################################################################

y_predict_result = model.predict(X_test)

fpr, tpr, thresholds = roc_curve (Y_test.ravel(), y_predict_result.ravel())
roc_auc = auc(fpr, tpr)

plt.clf()
plt.figure(figsize = (9, 7))
plt.plot(fpr, tpr, color='navy', lw=10, label='ROC Curve (AUC = %0.2f)' % roc_auc)
plt.plot([0, 1], [0, 1], color='red', lw=2, linestyle='--')
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.0])
plt.rcParams['font.size'] = 12
plt.title('[ ROC Curve ]')
plt.xlabel('Specificity-False Positive Rate')
plt.ylabel('Sensitivity-True Positive Rate')
plt.legend(loc="lower right")
plt.grid(True)
plt.show()

###그림 8.29 DNN 모델 저장##################################################################

from keras.models import load_model

model.save('C://models/smartcar_dnn_model.h5')
