pre_dir=run_new_nomasknum_fixcont_fixcomma_dedup_fixcomma2
dir=run_ft_new_nomasknum_fixcont_fixcomma_dedup_fixcomma2_newdata
mkdir -p $dir/res $dir/summary
CUDA_VISIBLE_DEVICES=1,2 torchrun \
 --nproc_per_node 2 \
 codet5_finetune_train.py \
 --task translate \
 --cache_path cache \
 --summary_dir summary \
 --data_dir xxx \
 --res_dir $dir/res \
 --output_dir $dir \
 --do_train \
 --do_eval \
 --train_batch_size 16 \
 --eval_batch_size 16 \
 --max_target_length 256 \
 --load_model_path $pre_dir/checkpoint-best-ppl/pytorch_model.bin
