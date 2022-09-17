dir=run_new_nomasknum_fixcont_fixcomma_dedup_fixcomma2

mkdir -p $dir/summary $dir/res

CUDA_VISIBLE_DEVICES=1,2 torchrun --nproc_per_node 2 codet5_pretrain.py --task summarize --cache_path cache --summary_dir summary --data_dir xxx --res_dir $dir/res --output_dir $dir --do_train --do_eval --train_batch_size 128 --eval_batch_size 64 # --load_model_path $dir/checkpoint-best-ppl/pytorch_model.bin
