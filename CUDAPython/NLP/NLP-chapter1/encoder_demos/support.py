import argparse



def get_args_dict():
    args = {}
    args['beam'] = 4
    args['bpe_codes'] = '/data/wmt14_en_de_joined_dict/code'
    args['buffer_size'] = 1
    args['cpu'] = False
    args['data'] = '/data/wmt14_en_de_joined_dict/'
    args['fp16'] = True
    args['fuse_dropout_add'] = True
    args['fuse_relu_dropout'] = False
    args['gen_subset'] = 'test'
    args['ignore_case'] = False
    args['left_pad_source'] = 'True'
    args['left_pad_target'] = 'False'
    args['lenpen'] = 1
    args['log_format'] = None
    args['log_interval'] = 1000
    args['max_len_a'] = 0
    args['max_len_b'] = 200
    args['max_sentences'] = None
    args['max_source_positions'] = 1024
    args['max_target_positions'] = 1024
    args['max_tokens'] = 128
    args['min_len'] = 1
    args['model_overrides'] = '{}'
    args['nbest'] = 1
    args['no_beamable_mm'] = False
    args['no_early_stop'] = False
    args['no_progress_bar'] = False
    args['num_shards'] = 1
    args['online_eval'] = False
    args['pad_sequence'] = 1
    args['path'] = '/data/checkpoints/JoC_Transformer_FP32_PyT_20190304.pt'
    args['prefix_size'] = 0
    args['print_alignment'] = False
    args['profile'] = None
    args['quiet'] = False
    args['raw_text'] = False
    args['remove_bpe'] = '@@ '
    args['replace_unk'] = None
    args['sampling'] = False
    args['sampling_temperature'] = 1
    args['sampling_topk'] = -1
    args['score_reference'] = False
    args['seed'] = 1
    args['sentencepiece'] = False
    args['shard_id'] = 0
    args['skip_invalid_size_inputs_valid_test'] = False
    args['source_lang'] = None
    args['target_lang'] = None
    args['task'] = 'translation'
    args['unkpen'] = 0
    args['unnormalized'] = False
    return args

def get_args():
    arg_dict = get_args_dict()
    args = argparse.Namespace()
    for key, val in arg_dict.items():
        setattr(args, key, val)
    return args