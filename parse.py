from collections import namedtuple
from functools import reduce
from glob import iglob
import json
import os
import pickle
from typing import NamedTuple, Union
from multiprocessing import Pool, Process
from tqdm import tqdm
from tree_sitter import Language, Parser, Node
import pandas as pd
from code import interact
from ts.parse import RUST_LANGUAGE

func_query = RUST_LANGUAGE.query("""
(function_item name: (identifier)) @functions
""")

parser = Parser()
parser.set_language(RUST_LANGUAGE)


def parse_func(contents: bytes) -> list[tuple[str, str]]:
    tree = parser.parse(contents)
    root = tree.root_node
    if root.type != "source_file":
        return []

    results = []
    captures: list[tuple[Node, str]] = func_query.captures(root)
    for node, tag in captures:
        prev = node
        while prev.prev_sibling and prev.prev_sibling.end_point[0] + 1 == prev.start_point[0] and prev.prev_sibling.type == "attribute_item":
            prev = prev.prev_sibling

        code = contents[prev.start_byte: node.end_byte]

        comment = b''
        if prev.prev_sibling and prev.prev_sibling.end_point[0] + 1 == prev.start_point[0] and prev.prev_sibling.type == "line_comment":
            comm = prev.prev_sibling
            prev = comm
            while prev.prev_sibling and prev.prev_sibling.end_point[0] + 1 == prev.start_point[0] and prev.prev_sibling.type == "line_comment":
                prev = prev.prev_sibling

            comment = contents[prev.start_byte: comm.end_byte]

        try:
            results.append((comment.decode('utf-8'), code.decode('utf-8')))
        except UnicodeDecodeError:
            print("!", end='')
            pass

    return results


class CodeDocData(NamedTuple):
    repo: str
    ref: str
    file: str
    doc: str
    code: str


def rs_repo_iter():
    repos = []
    for p, d, f in os.walk("crawler/crawl-10star-new"):
        for file in f:
            if file.endswith('.jsonl'):
                with open(os.path.join(p, file), 'r') as f:
                    for line in f:
                        js = json.loads(line)
                        if js["stargazers_count"] >= 10 and js['fork'] == False:
                            repos.append((js["name"], js['full_name']))

    for name, full_name in repos:
        yield name, full_name


def rs_file_iter(repo, full_name):
    print(full_name)
    repo_dir = "/data1/zixuantan/rust_repos/" + full_name.replace("/", "+")

    ref = os.popen(f"git -C {repo_dir} rev-parse HEAD").read().strip()

    for p, d, f in os.walk(repo_dir):
        for file in f:
            if file.endswith(".rs"):
                path = os.path.join(p, file)
                path = os.path.abspath(path)

                # filter out the links and directories
                if not os.path.isfile(path):
                    continue

                yield full_name, ref, path


def handler(args):
    repo, full_name = args
    iter = rs_file_iter(repo, full_name)

    res = []
    for full_name, ref, path in iter:
        with open(path, 'rb') as f:
            contents = f.read()
        res.extend(CodeDocData(full_name, ref, path, comment, code) for comment, code in parse_func(contents))

    with open("/tmp/" + full_name.replace("/", "+") + ".pickle", "wb") as f:
        pickle.dump(res, f)


def main():
    riter = list(rs_repo_iter())
    print("len", len(riter))

    with Pool() as pool:
        pool.map(handler, riter)

    print("Merge")
    data = []
    for _, full_name in riter:
        with open("/tmp/" + full_name.replace("/", "+") + ".pickle", "rb") as f:
            res = pickle.load(f)
            data.extend(res)

    df = pd.DataFrame(data)
    df.to_parquet("/data1/zixuantan/github/data-new/codedocdata.parquet")


# path = '/data1/zixuantan/rust_repos/POC-polkadai-bridge/bridge/validator/vendor/substrate-api-client/src/examples/extrinsic/mod.rs'
# with open(path, 'rb') as f:
#     contents = f.read()
# parse_func(contents)


if __name__ == "__main__":
    main()
