import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv(r"TrainingData\TrainingData.csv", parse_dates=["time_berlin"])
df = df.set_index("time_berlin").sort_index()
df.head()

df.plot(subplots=True, figsize=(12,8))
plt.show()