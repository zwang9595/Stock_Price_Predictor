import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import precision_score
from utilities import status_calc

def backtest():

    data_df = pd.read_csv("data/ks.csv", index_col="Date")
    data_df.dropna(axis=0, how="any", inplace=True)

    features = data_df.columns[6:]
    X = data_df[features].values

    y = list(
        status_calc(
            data_df["stock_p_change"], data_df["SP500_p_change"], outperformance=10
        )
    )

    z = np.array(data_df[["stock_p_change", "SP500_p_change"]])

    X_train, X_test, y_train, y_test, z_train, z_test = train_test_split(
        X, y, z, test_size=0.2
    )

    clf = RandomForestClassifier(n_estimators=100, random_state=0)
    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)
    print("Classifier performance\n", "=" * 20)
    print(f"Accuracy score: {clf.score(X_test, y_test): .2f}")
    print(f"Precision score: {precision_score(y_test, y_pred): .2f}")

    num_positive_predictions = sum(y_pred)
    if num_positive_predictions < 0:
        print("No stocks predicted!")

    stock_returns = 1 + z_test[y_pred, 0] / 100
    market_returns = 1 + z_test[y_pred, 1] / 100

    avg_predicted_stock_growth = sum(stock_returns) / num_positive_predictions
    index_growth = sum(market_returns) / num_positive_predictions
    percentage_stock_returns = 100 * (avg_predicted_stock_growth - 1)
    percentage_market_returns = 100 * (index_growth - 1)
    total_outperformance = percentage_stock_returns - percentage_market_returns

    print("\n Stock prediction performance report \n", "=" * 40)
    print(f"Total Trades:", num_positive_predictions)
    print(f"Average return for stock predictions: {percentage_stock_returns: .1f} %")
    print(
        f"Average market return in the same period: {percentage_market_returns: .1f}% "
    )
    print(
        f"Compared to the index, our strategy earns {total_outperformance: .1f} percentage points more"
    )

if __name__ == "__main__":
    backtest()
