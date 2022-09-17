from code import interact
import pandas as pd
import torch
odf = pd.read_parquet("data/codedocdata.parquet")
df = pd.read_parquet("data/codedocdata.processed.parquet")
if len(df.drop_duplicates(['code'])) != len(df):
    print("processed dataset has duplicates")

# a = df[df.doc.map(lambda s: '\t' in s)]
# if len(a) > 0:
#     print("tab in doc")
# a = df[df.doc.map(lambda s: '\n' in s)]
# if len(a) > 0:
#     print("newline in doc")
# a = df[df.code.map(lambda s: '\t' in s)]
# if len(a) > 0:
#     print("tab in code")
# a = df[df.code.map(lambda s: '\n' in s)]
# if len(a) > 0:
#     print("newline in code")
# a = df[df.doctest.map(lambda s: '\t' in s)]
# if len(a) > 0:
#     print("tab in doctest")
# a = df[df.doctest.map(lambda s: '\n' in s)]
# if len(a) > 0:
#     print("newline in doctest")
# codet5test = torch.load("data/codedocdata.processed.codet5.test.examples")

pretrain_df = pd.read_parquet("data/codedocdata.processed.codet5.parquet")
if len(pretrain_df.drop_duplicates(['code', 'doc'])) != len(pretrain_df):
    print("pretrain dataset has duplicates")

finetune_df = pd.read_parquet("data/codedocdata.processed.codet5.finetune.parquet")
if len(finetune_df.drop_duplicates(['code', 'doc', 'doctest'])) != len(finetune_df):
    print("finetune dataset has duplicates")

interact(local=locals())
