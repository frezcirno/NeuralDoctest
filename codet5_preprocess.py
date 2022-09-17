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
parser.add_argument("--max_source_length", default=64, type=int)
parser.add_argument("--max_target_length", default=32, type=int)
args = parser.parse_args(args=[])


data_dir = "/data1/zixuantan/github/data-new"
data = data_dir + "/codedocdata.processed.parquet"

tokenizer: RobertaTokenizer = RobertaTokenizer.from_pretrained('Salesforce/codet5-base')


def process(path):
    df = pd.read_parquet(path)
    df['code_tokens'] = df.code.apply(lambda x: tokenizer.encode(
        x, max_length=args.max_source_length, padding='max_length', truncation=True))
    df['doc_tokens'] = df.doc.apply(lambda x: tokenizer.encode(
        x, max_length=args.max_target_length, padding='max_length', truncation=True))
    df.to_parquet(path)


codedocdata = pd.read_parquet(data)


def keep(doc: str) -> bool:
    if isinstance(doc, str):
        return len(doc) >= 3 and '\t' not in doc
    return False


codedocdata = codedocdata[codedocdata.doc.map(keep)]
codedocdata = codedocdata.drop_duplicates(subset=['code', 'doc'])


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
codedocdata.to_parquet(data_dir + "/codedocdata.processed.codet5.parquet")

# Clean up
print("Cleaning up...")
for path in paths:
    os.remove(path)


dataset = TensorDataset(torch.tensor(np.stack(codedocdata.code_tokens.values)),
                        torch.tensor(np.stack(codedocdata.doc_tokens.values)))

train_size = int(0.8 * len(dataset))
valid_size = int(0.1 * len(dataset))
test_size = len(dataset) - train_size - valid_size
splits = random_split(dataset, [train_size, valid_size, test_size],
                      generator=torch.Generator().manual_seed(42))

for label, split in zip(['train', 'valid', 'test'], splits):
    torch.save(TensorDataset(*split[:]), data_dir + f"/codedocdata.processed.codet5.{label}.dataset")

valid_df = codedocdata.iloc[splits[1].indices]
torch.save([Example(row.Index, row.code, row.doc) for row in valid_df.itertuples()],
           data_dir + "/codedocdata.processed.codet5.valid.examples")

test_df = codedocdata.iloc[splits[2].indices]
torch.save([Example(row.Index, row.code, row.doc) for row in test_df.itertuples()],
           data_dir + "/codedocdata.processed.codet5.test.examples")
