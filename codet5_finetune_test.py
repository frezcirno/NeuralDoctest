from code import interact
import os
import numpy as np
import pandas as pd
import torch
from evaluator import smooth_bleu
from evaluator.CodeBLEU import calc_code_bleu
from evaluator.bleu import _bleu
from _utils import Example

sdf: list[Example] = torch.load('data-new/codedocdata.processed.codet5.finetune.test.examples')
sdf = {ex.idx: ex for ex in sdf}
df = pd.read_parquet("run_ft/res/pred_test.parquet")

output_fn_idx = "/tmp/test_best-bleu.output"
gold_fn_idx = "/tmp/test_best-bleu.gold"
src_fn_idx = "/tmp/test_best-bleu.src"
output_fn_noidx = "/tmp/test_best-bleu.noidx.output"
gold_fn_noidx = "/tmp/test_best-bleu.noidx.gold"
src_fn_noidx = "/tmp/test_best-bleu.noidx.src"

dev_accs, predictions = [], []
with open(output_fn_idx, 'w') as f, open(gold_fn_idx, 'w') as f1, open(src_fn_idx, 'w') as f2, \
        open(output_fn_noidx, 'w') as ff, open(gold_fn_noidx, 'w') as ff1, open(src_fn_noidx, 'w') as ff2:
    for it in df.itertuples():
        idx = it.Index
        gold = it.gold.replace("\n", " ")
        pred = it.pred.replace("\n", " ")
        src = sdf[idx].source.replace("\n", " ")

        dev_accs.append(pred.strip() == gold.strip())

        # for smooth-bleu4 evaluation
        predictions.append(str(idx) + '\t' + pred)

        f.write(str(idx) + '\t' + pred.strip() + '\n')
        f1.write(str(idx) + '\t' + gold.strip() + '\n')
        f2.write(str(idx) + '\t' + src.strip() + '\n')

        ff.write(pred.strip() + '\n')
        ff1.write(gold.strip() + '\n')
        ff2.write(src.strip() + '\n')

bleu = round(_bleu(gold_fn_noidx, output_fn_noidx), 2)
codebleu = calc_code_bleu.get_codebleu(gold_fn_noidx, output_fn_noidx, 'rust')

result = {'em': np.mean(dev_accs) * 100, 'bleu': bleu}
result['codebleu'] = codebleu * 100

print(result)
