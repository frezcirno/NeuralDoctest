from code import interact
from multiprocessing import Pool
import shutil
import stat
import subprocess
import pandas as pd
import os
from ts.parse import parse_ast


def replace_general_string_tok(tok: str) -> str:
    return (
        tok.replace(" STRNEWLINE ", "\n")
        .replace(" TABSYMBOL ", "\t")
        .replace(" ", "")
        .replace("▁", " ")
    )


def process_string(s: str):
    """ Process a string literal. """
    return replace_general_string_tok(s)


def sed_replace(file_path: str, old_str: str, new_str: str) -> None:
    """ Replace a string in a file. """
    with open(file_path, 'r') as f:
        texts = f.read()
        if old_str in texts:
            texts = texts.replace(old_str, new_str)
    with open(file_path, 'w') as f:
        f.write(texts)


def find_cargo_toml(path: str) -> str:
    rv = os.lstat(path)
    if not stat.S_ISDIR(rv.st_mode):
        path = os.path.dirname(path)
    while True:
        if "Cargo.toml" in os.listdir(path):
            return os.path.join(path, "Cargo.toml")
        path = os.path.dirname(path)
        if os.path.samefile(path, all_repo_base):
            return ""


def run_test(row):
    idx = row.Index
    # repo = odf.loc[idx].repo
    file = odf.loc[idx].file
    file_base = os.path.dirname(file)
    # repo_base = os.path.join(all_repo_base, repo.split("/")[1])

    # subprocess.Popen([git, "clean", "-f"], cwd=repo_base, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()
    # subprocess.Popen([git, "restore", "."], cwd=repo_base, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()

    fgenerated = os.path.join(file_base, f"codet5_generated_{idx}.rs")
    ferr = os.path.join(file_base, f"codet5_generated_{idx}.err")
    foutput = os.path.join(file_base, f"codet5_generated_{idx}.output")
    fgold = os.path.join(file_base, f"codet5_generated_{idx}.gold")

    print(fgenerated)

    fcargo = find_cargo_toml(file)
    if not fcargo:
        print(f"{file} has no Cargo.toml")
        return False
    crate_base = os.path.dirname(fcargo)
    # subprocess.Popen([cargo, "clean"], cwd=crate_base, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()

    directive = f"\n[[bin]]\nname = \"codet5_generated_{idx}\"\npath = \"{fgenerated}\"\n"
    with open(fcargo, "r") as f:
        content = f.read()
    if f"codet5_generated_{idx}" in content:
        sed_replace(fcargo, directive, "")
    with open(fcargo, "a") as f:
        print(directive, file=f)

    with open(fgenerated, "w") as f:
        hasmain = 'fn main' in row.pred
        if hasmain:
            print(row.pred, file=f)
        else:
            lines = row.pred.split(';')
            uses = []
            nonuses = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                if line.startswith("#") or line.startswith("use"):
                    uses.append(line)
                else:
                    nonuses.append(line)
            ## Copy the use statements in corrisponding unit
            # with open(file, 'r') as target:
            #     start = 0
            #     for line in target:
            #         if start or line.startswith("use "):
            #             f.write(line)
            #             start = 1
            #         if ';' in line:
            #             start = 0
            ## Copy generated use statements
            for line in uses:
                f.write(line.lstrip(" #"))
                f.write(";\n")
            f.write("pub fn main() {\n")
            for line in nonuses:
                f.write(line)
                f.write(";\n")
            f.write("\n}")

    p = subprocess.Popen([cargo, "build", "--bin", f"codet5_generated_{idx}"],
                         cwd=crate_base, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    res = p.wait()

    subprocess.Popen([cargo, "clean"], cwd=crate_base, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()

    if res == 0:
        return True

    with open(ferr, "wb") as f:
        f.write(out)
        f.write(err)
    with open(foutput, "w") as f:
        f.write(row.pred)
    with open(fgold, "w") as f:
        f.write(row.gold)
    return False


def process(path):
    df = pd.read_parquet(path)
    succ = 0
    total = 0
    for row in df.itertuples():
        rv = run_test(row)
        if rv:
            print('.', end='')
            succ += 1
        total += 1
    return succ, total


rustc = shutil.which('rustc')
cargo = shutil.which('cargo')
git = shutil.which('git')
odf = pd.read_parquet("data-new/codedocdata.parquet")
all_repo_base = "/data1/zixuantan/rust_repos/"
df = pd.read_parquet("run_ft/res/pred_test.parquet")

# Split the dataframe into multiple dataframes
paths = []
for i in range(0, df.shape[0], 50):
    path = f'/tmp/part_{i}.parquet'
    df.iloc[i:i + 50].to_parquet(path)
    paths.append(path)
print(f"{len(paths)} files created")


with Pool(4) as pool:
    res = pool.map(process, paths)

succ = 0
total = 0
for r in res:
    succ += r[0]
    total += r[1]
print(succ, total, succ / total)
interact(local=locals())
