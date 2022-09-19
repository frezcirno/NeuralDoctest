from code import interact
import shutil
import subprocess
import pandas as pd
import os

res_dir = "run_ft_new_nomasknum_fixcont_fixcomma_dedup_fixcomma2_newdata/res"
criteria = 'bleu'
output_fn = os.path.join(res_dir, "test_best-{}.output".format(criteria))
gold_fn = os.path.join(res_dir, "test_best-{}.gold".format(criteria))
src_fn = os.path.join(res_dir, "test_best-{}.src".format(criteria))

outputs = []
indices = []
with open(output_fn, 'r') as f, open(gold_fn, 'r') as f1, open(src_fn, 'r') as f2:
    for output, gold, src in zip(f, f1, f2):
        idx = int(src.split('\t')[0])
        indices.append(idx)
        src = src.split('\t')[1]
        output = output.split('\t')[1]
        gold = gold.split('\t')[1]
        outputs.append({"src": src, "output": output, "gold": gold})

df = pd.DataFrame(outputs, indices)
df.to_parquet("data-new/test_best-{}.parquet".format(criteria))

exit(0)

interact(local=locals())

rustc = shutil.which('rustc')
ok = 0
total = 0

for row in df.itertuples():
    os.makedirs("codes/", exist_ok=True)
    fname = f"codes/{row.Index}.rs"
    with open(fname, "w") as f:
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

    p = subprocess.Popen([rustc, fname, "-o", f"bins/{row.Index}"],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    res = p.wait()
    if res == 0:
        ok += 1
    else:
        with open(f"codes/{row.Index}.err", "wb") as f:
            f.write(out)
            f.write(err)
        with open(f"codes/{row.Index}.gold", "w") as f:
            f.write(row.gold)
        with open(f"codes/{row.Index}.ori", "w") as f:
            f.write(row.output)
    total += 1
    print("{}/{}".format(ok, total))

interact(local=locals())
