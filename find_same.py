
from code import interact
import os
from random import randint
import sys
import pandas as pd
from fuzzywuzzy import fuzz


df = pd.read_parquet("doctest.parquet")
sample = df.iloc[randint(0, len(df))][['code', 'doccode']]

with open("sample.rust", "w") as f:
    print(sample.code, file=f)
    for dc in sample.doccode:
        print(dc, file=f)

if os.path.exists("same.txt"):
    with open("same.txt", "r") as f:
        same = []
        for line in f:
            i, j = line.strip().split()
            same.append((int(i), int(j)))

    parent = [i for i in range(len(df))]

    def root(i):
        if parent[i] == i:
            return i
        else:
            parent[i] = root(parent[i])
            return parent[i]

    for i, j in same:
        parent[i] = root(j)

    a = []
    for i in range(len(df)):
        if root(i) == i:
            a.append(i)

    with open("unique.txt", "w") as f:
        for i in a:
            print(i, file=f)

else:
    same = []
    for i in range(len(df)):
        s1 = df.iloc[i]
        for j in range(i+1, len(df)):
            s2 = df.iloc[j]
            if s1.code == s2.code:
                # if fuzz.ratio(s1.code, s2.code) > 85:
                print(i, j)
                same.append((i, j))
    with open("same.txt", "w") as f:
        for i, j in same:
            print(i, j, file=f)

interact(local=locals())
