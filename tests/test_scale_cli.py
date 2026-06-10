import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import pytest
from rate_limiter_demo import build_arg_parser

VALID_STRATEGIES = {"sliding_window", "token_bucket", "token_aware", "all"}


def test_default_args():
    parser = build_arg_parser()
    args = parser.parse_args([])
    assert args.scale is False
    assert args.strategy == "all"


def test_scale_flag():
    parser = build_arg_parser()
    args = parser.parse_args(["--scale"])
    assert args.scale is True


def test_strategy_choices():
    parser = build_arg_parser()
    for s in VALID_STRATEGIES:
        args = parser.parse_args(["--scale", "--strategy", s])
        assert args.strategy == s


def test_invalid_strategy_raises():
    parser = build_arg_parser()
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["--scale", "--strategy", "invalid"])
    assert exc_info.value.code == 2
