import matplotlib.pyplot as plt
import pandas as pd

# a simple line plot
scores = """
     0.5 |  1692
     0.6 |  3161
     0.7 |  2293
     0.8 |  1668
     0.9 |  1300
     1.0 |  2203
     1.1 |  2264
     1.2 |  2559
     1.3 |  2254
     1.4 |  1518
     1.5 |  1808
     1.6 |  2210
     1.7 |  2045
     1.8 |  1515
     1.9 |   314
     2.0 |  1696
"""

lines = scores.split("\n")

scores = []
num_scores = []
for line in lines:
    line = line.strip()
    if not line:
        continue
    parts = line.split("|")
    score = float(parts[0].strip())
    num = int(parts[1].strip())
    scores.append(score)
    num_scores.append(num)
    print(f"score: {score} num: {num}")


df = pd.DataFrame({
    'scores': scores,
    'count': num_scores
})


the_plot = df.plot(kind='bar', x='scores', y='count')
fig = the_plot.get_figure()
fig.savefig('./figure.pdf')

