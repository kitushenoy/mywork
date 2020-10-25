"""
Command:
python interactive.py --buffer-size 1 --fp16 --path /workspace/checkpoints/checkpoint_best.pt --max-tokens 128
    --fuse-dropout-add /data/wmt14_en_de_joined_dict/

Or:
python interactive.py --buffer-size 1 --fp16 --path /data/checkpoints/JoC_Transformer_FP32_PyT_20190304.pt --max-tokens 128 \
    --fuse-dropout-add --remove-bpe \
    --bpe-codes /data/wmt14_en_de_joined_dict/code \
    /data/wmt14_en_de_joined_dict/

# Example command
"""

from collections import namedtuple
import numpy as np
import sys
import torch
import encoder_demos.support as support

from fairseq import data, options, tasks, tokenizer, utils
from fairseq.sequence_generator import SequenceGenerator

from apply_bpe import BPE

Batch = namedtuple('Batch', 'srcs tokens lengths')
Translation = namedtuple('Translation', 'src_str hypos pos_scores alignments')

def buffered_read(input_text, buffer_size):
    buffer = []
    for src_str in input_text:
        buffer.append(src_str.strip())
        if len(buffer) >= buffer_size:
            yield buffer
            buffer = []

    if len(buffer) > 0:
        yield buffer


def make_batches(lines, args, src_dict, max_positions, bpe=None):

    # We are making batches
    tokens = [
        tokenizer.Tokenizer.tokenize(src_str, src_dict, tokenize=tokenizer.tokenize_en, add_if_not_exist=False, bpe=bpe).long()
        for src_str in lines
    ]
    return tokens



def demo(input_text):
    assert input_text != "", "Input text must contain at least one character."

    args = support.get_args()

    task = tasks.setup_task(args)
    maxposes = (args.max_source_positions, args.max_target_positions)

    # # Set dictionaries
    src_dict = task.source_dictionary

    # Load BPE codes file
    if args.bpe_codes:
        codes = open(args.bpe_codes, 'r')
        bpe = BPE(codes)

    if args.buffer_size > 1:
        print('| Sentence buffer size:', args.buffer_size)

    for inputs in buffered_read([input_text], args.buffer_size):
        tokens = make_batches(inputs, args, src_dict, maxposes, bpe)
        return tokens

