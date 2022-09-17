dir=run_ft_new_nomasknum_fixcont_fixcomma_dedup_fixcomma2
cp -r $dir/checkpoint-best-ppl $dir/checkpoint-best-bleu
CUDA_VISIBLE_DEVICES=0 python codet5_finetune_train.py --task summarize --lang rust --cache_path cache --summary_dir summary --data_dir xxx --res_dir $dir/res --output_dir $dir --train_batch_size 16 --eval_batch_size 16 --max_target_length 256 --load_model_path $dir/checkpoint-best-bleu/pytorch_model.bin --do_test
