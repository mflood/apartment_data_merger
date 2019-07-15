import matplotlib.pyplot as plt
import pandas as pd

# a simple line plot
scores = """
     0.5 |  3606
     0.6 |  6785
     0.7 |  4995
     0.8 |  3508
     0.9 |  2606
     1.0 |  4131
     1.1 |  3915
     1.2 |  4175
     1.3 |  3743
     1.4 |  2628
     1.5 |  3339
     1.6 |  3707
     1.7 |  3283
     1.8 |  2527
     1.9 |   546
     2.0 |  3429
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

