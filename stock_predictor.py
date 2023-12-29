import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from utilities import data_string_to_float, status_calc

OUTPERFORMANCE = 10

def build_data_set():

    training_data = pd.read_csv("data/ks.csv", index_col="Date")
    training_data.dropna(axis=0, how="any", inplace=True)
    features = training_data.columns[6:]

    X_train = training_data[features].values
    y_train = list(
        status_calc(
            training_data["stock_p_change"],
            training_data["SP500_p_change"],
            OUTPERFORMANCE,
        )
    )

    return X_train, y_train

def predict_stocks():
    X_train, y_train = build_data_set()
    clf = RandomForestClassifier(n_estimators=100, random_state=0)
    clf.fit(X_train, y_train)

    data = pd.read_csv("data/fs.csv", index_col="Date")
    data.dropna(axis=0, how="any", inplace=True)
    features = data.columns[6:]
    X_test = data[features].values
    z = data["Ticker"].values

    y_pred = clf.predict(X_test)
    if sum(y_pred) == 0:
        print("No stocks predicted!")
    else:
        invest_list = z[y_pred].tolist()
        print(
            f"{len(invest_list)} stocks predicted to outperform by more than {OUTPERFORMANCE}%:"
        )
        print(" ".join(invest_list))
        return invest_list

if __name__ == "__main__":
    print("Building dataset and predicting stocks...")
    predict_stocks()
