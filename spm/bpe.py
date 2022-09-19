
import os
from sentencepiece import SentencePieceProcessor

DIR = os.path.dirname(os.path.abspath(__file__))

tokenizer = SentencePieceProcessor(model_file=DIR + "/rust.bpe.model")

pad_id = tokenizer.pad_id()


def encode(x, max_length, padding=True):
    ids = tokenizer.EncodeAsIds(x, add_bos=True, add_eos=True)
    if len(ids) > max_length:
        ids = ids[:max_length - 1] + [ids[-1]]
    if padding and len(ids) < max_length:
        ids += [tokenizer.pad_id()] * (max_length - len(ids))
    return ids


def decode(tokens):
    tokens = [t for t in tokens if t != tokenizer.pad_id()]
    return tokenizer.DecodeIds(tokens)
