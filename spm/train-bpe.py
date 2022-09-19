import argparse
import os
from sentencepiece import SentencePieceTrainer
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--input", help="The file to parse", default="data-new/codedocdata.processed.parquet")
parser.add_argument("--prefix", help="", default='rust.bpe')
args = parser.parse_args()

df = pd.read_parquet(args.input)

with open("/tmp/rust-code.txt", "w") as code, open("/tmp/comment.txt", "w") as doc, open("/tmp/rust-doctest.txt", "w") as doctest:
    for t in df.itertuples():
        code.write(t.code.replace("\n", " "))
        code.write("\n")
        doc.write(t.doc.replace("\n", " "))
        doc.write("\n")
        doctest.write(t.doctest.replace("\n", " "))
        doctest.write("\n")

FILES = [
    "/tmp/rust-code.txt",
    "/tmp/rust-doctest.txt",
    "/tmp/comment.txt",
]

SentencePieceTrainer.Train(
    "--input={} --vocab_size=50000 --model_prefix={} --bos_id=1 --eos_id=2 --pad_id=0 --unk_id=3 "
    "--input_sentence_size=10000000 --shuffle_input_sentence=true --hard_vocab_limit=false "
    "--character_coverage=1.0  --model_type=bpe".format(",".join(FILES), args.prefix))

for f in FILES:
    os.remove(f)
