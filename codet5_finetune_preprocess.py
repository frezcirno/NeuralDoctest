import argparse
from code import interact
from itertools import repeat
from multiprocessing import Pool
import os
import numpy as np
import pandas as pd
import torch
from transformers import RobertaTokenizer
from torch.utils.data import TensorDataset, random_split
from _utils import Example

parser = argparse.ArgumentParser(description="")
parser.add_argument("--max_code_length", default=256, type=int)
parser.add_argument("--max_doc_length", default=32, type=int)
parser.add_argument("--max_target_length", default=256, type=int)
args = parser.parse_args(args=[])


data_dir = "/data1/zixuantan/github/data-new"
data = data_dir + "/codedocdata.processed.parquet"

tokenizer: RobertaTokenizer = RobertaTokenizer.from_pretrained('Salesforce/codet5-base')


def process(path):
    df = pd.read_parquet(path)
    code_tokens = df.code.apply(lambda x: tokenizer.encode(
        x, max_length=args.max_code_length, padding='max_length', truncation=True))
    doc_tokens = df.doc.apply(lambda x: tokenizer.encode(
        x, max_length=args.max_doc_length, padding='max_length', truncation=True))
    df['code_doc_tokens'] = code_tokens + doc_tokens
    df['doctest_tokens'] = df.doctest.apply(lambda x: tokenizer.encode(
        x, max_length=args.max_target_length, padding='max_length', truncation=True))
    df.to_parquet(path)


codedocdata = pd.read_parquet(data)
# interact(local=locals())

# Filter out the examples without doctest
codedocdata = codedocdata[codedocdata.doctest.map(lambda s: s != '')]
codedocdata = codedocdata.drop_duplicates(subset=['code', 'doc', 'doctest'])

paths = []
for i in range(0, codedocdata.shape[0], 10000):
    path = f'/tmp/part_{i}.parquet'
    codedocdata.iloc[i:i + 10000].to_parquet(path)
    paths.append(path)
print(f"{len(paths)} files created")

# Process the dataframe
with Pool() as p:
    p.map(process, paths)

# Merge the parts
print("Merging...")
codedocdata = pd.concat([pd.read_parquet(path) for path in paths])
codedocdata.to_parquet(data_dir + "/codedocdata.processed.codet5.finetune.parquet")

# Clean up
print("Cleaning up...")
for path in paths:
    os.remove(path)


dataset = TensorDataset(torch.tensor(np.stack(codedocdata.code_doc_tokens.values)),
                        torch.tensor(np.stack(codedocdata.doctest_tokens.values)))

train_size = int(0.8 * len(dataset))
valid_size = int(0.1 * len(dataset))
test_size = len(dataset) - train_size - valid_size
splits = random_split(dataset, [train_size, valid_size, test_size],
                      generator=torch.Generator().manual_seed(42))

for label, split in zip(['train', 'valid', 'test'], splits):
    torch.save(TensorDataset(*split[:]), data_dir + f"/codedocdata.processed.codet5.finetune.{label}.dataset")

valid_df = codedocdata.iloc[splits[1].indices]
torch.save([Example(row.Index, row.code + row.doc, row.doctest) for row in valid_df.itertuples()],
           data_dir + "/codedocdata.processed.codet5.finetune.valid.examples")

test_df = codedocdata.iloc[splits[2].indices]
torch.save([Example(row.Index, row.code + row.doc, row.doctest) for row in test_df.itertuples()],
           data_dir + "/codedocdata.processed.codet5.finetune.test.examples")

# interact(local=locals())
