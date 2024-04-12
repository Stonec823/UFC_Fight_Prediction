import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split # type: ignore
from sklearn.preprocessing import StandardScaler# type: ignore
from statsmodels.stats.outliers_influence import variance_inflation_factor # type: ignore
from sklearn import svm# type: ignore
from sklearn import metrics# type: ignore


df = pd.read_csv('/Users/cstone/Documents/School/MGMT 6203/Project/clean/final_features.csv')


vif_data = pd.DataFrame() 
vif_data["feature"] = df.columns
vif_data["VIF"] = [variance_inflation_factor(df.values, i) 
                          for i in range(len(df.columns))] 
  
cols_to_keep = vif_data[(vif_data['VIF'] <= 5) | (vif_data['feature'] == 'W_f2') | (vif_data['feature'] == 'L_f2') ]
cols_to_keep = cols_to_keep['feature'].to_list()
new_df = df[cols_to_keep]



#creating response variable

x = new_df.drop('outcome', axis=1)
y = new_df['outcome']


#scaling features

scale = StandardScaler()
scaledX = scale.fit_transform(x)


#splitting data 70/30 and running SVM linear classifier

X_train, X_test, y_train, y_test = train_test_split(scaledX, y, test_size=0.3, random_state = 123)
clf = svm.SVC(kernel='linear')

clf.fit(X_train,y_train)
y_pred = clf.predict(X_test)
print(metrics.accuracy_score(y_test, y_pred))