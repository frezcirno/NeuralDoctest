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
        tok.replace(" ▁ ", " ")
        .replace(" STRNEWLINE ", "\n")
        .replace(" TABSYMBOL ", "\t")
    )


def process_string(s: str):
    """ Process a string literal. """
    return replace_general_string_tok(s)


def parse_code(code: str) -> str:
    """ Parse a code block and return a list of tokens. """
    bcode = code.encode('utf-8')
    tree = parse_ast(bcode)

    # retrival
    nodes = []
    nodes_to_expand = [tree.root_node]
    while nodes_to_expand:
        node = nodes_to_expand.pop(0)
        if node.type == 'string_literal':
            nodes.append(node)
            continue

        if not node.children and node.start_byte < node.end_byte:
            nodes.append(node)
            continue

        nodes_to_expand = node.children + nodes_to_expand

    # process
    tokens = []
    for node in nodes:
        text = bcode[node.start_byte:node.end_byte].decode('utf-8')

        if node.type == 'string_literal' or node.type == 'raw_string_literal':
            tokens.append(process_string(text))

        else:
            tokens.append(text)
    return " ".join(tokens)


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


rustc = shutil.which('rustc')
cargo = shutil.which('cargo')
git = shutil.which('git')
odf = pd.read_parquet("data/codedocdata.parquet")
all_repo_base = "/data/zixuantan/rust_repos/"


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

    fcargo = find_cargo_toml(file)
    if not fcargo:
        print(f"{file} has no Cargo.toml")
        return False
    crate_base = os.path.dirname(fcargo)
    subprocess.Popen([cargo, "clean"], cwd=crate_base, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).wait()

    directive = f"\n[[bin]]\nname = \"codet5_generated_{idx}\"\npath = \"{fgenerated}\"\n"
    with open(fcargo, "r") as f:
        content = f.read()
    if f"codet5_generated_{idx}" in content:
        sed_replace(fcargo, directive, "")
    with open(fcargo, "a") as f:
        print(directive, file=f)

    with open(fgenerated, "w") as f:
        hasmain = 'fn main' in row.output
        if hasmain:
            print(row.output, file=f)
        else:
            lines = row.output.split(';')
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
        f.write(row.output)
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
            succ += 1
        total += 1
    return succ, total


df = pd.read_parquet("data/test_best-bleu.parquet")

df['gold'] = df['gold'].apply(parse_code)
df['output'] = df['output'].apply(parse_code)

# Split the dataframe into multiple dataframes
paths = []
for i in range(0, df.shape[0], 50):
    path = f'data/tmp/part_{i}.parquet'
    df.iloc[i:i + 50].to_parquet(path)
    paths.append(path)
print(f"{len(paths)} files created")


with Pool() as pool:
    res = pool.map(process, paths)

succ = 0
total = 0
for r in res:
    succ += r[0]
    total += r[1]
print(succ, total, succ / total)
interact(local=locals())
