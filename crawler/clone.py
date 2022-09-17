import json
import os
import shutil
from tqdm import tqdm
from multiprocessing import Pool


def clone(item):
    if item["fork"]:
        return
    full_name = item["full_name"]
    clone_path = f'/data1/zixuantan/rust_repos/{full_name.replace("/", "+")}'
    if item["stargazers_count"] < 10:
        if os.path.exists(clone_path):
            print("Clean", full_name)
            shutil.rmtree(clone_path, ignore_errors=True)
        return
    print(f"Cloning {full_name} with {item['stargazers_count']} stars")
    os.system(
        f'git clone --depth 1 git@github.com:{full_name} {clone_path}')


wd = "crawl-10star-new/"

item_list = []
for p, d, files in os.walk(wd):
    for file in files:
        if file.endswith(".jsonl"):
            with open(os.path.join(p, file), "r") as fin:
                for line in fin:
                    item_list.append(json.loads(line))

item_list = sorted(item_list, key=lambda x: x["stargazers_count"], reverse=True)


with Pool(os.cpu_count() // 2) as pool:
    pool.map(clone, item_list)
