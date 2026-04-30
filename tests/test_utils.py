import pytest
from slurm_exporter.utils import (
    coerce_int,
    parse_gres_gpu_count,
    parse_gres_gpu_count_from_tres,
    parse_mem_to_bytes,
)


class TestParseGresGpuCount:
    def test_simple(self):
        assert parse_gres_gpu_count("gpu:4") == 4

    def test_named_model(self):
        assert parse_gres_gpu_count("gpu:a100:4") == 4

    def test_sxm5_with_idx(self):
        assert parse_gres_gpu_count("gpu:h100:sxm5:8(IDX:0-7)") == 8

    def test_model_with_punctuation(self):
        assert parse_gres_gpu_count("gpu:nvidia-a100-sxm4.40gb:8(IDX:0-7)") == 8

    def test_multi_gres(self):
        assert parse_gres_gpu_count("gpu:a100:2,gpu:h100:2") == 4

    def test_null(self):
        assert parse_gres_gpu_count("(null)") == 0

    def test_empty(self):
        assert parse_gres_gpu_count("") == 0

    def test_no_gpu(self):
        assert parse_gres_gpu_count("cpu:4") == 0


class TestParseGresGpuCountFromTres:
    def test_basic(self):
        assert parse_gres_gpu_count_from_tres("gres/gpu=8,cpu=256,mem=500G") == 8

    def test_named_model(self):
        assert parse_gres_gpu_count_from_tres("gres/gpu:a100=4") == 4

    def test_named_model_compound(self):
        assert parse_gres_gpu_count_from_tres("gres/gpu:h100:sxm5=8") == 8

    def test_no_gpu(self):
        assert parse_gres_gpu_count_from_tres("cpu=8,mem=32G") == 0

    def test_empty(self):
        assert parse_gres_gpu_count_from_tres("") == 0


class TestParseMemToBytes:
    def test_gigabytes(self):
        assert parse_mem_to_bytes("128G") == 128 * 1024 ** 3

    def test_megabytes(self):
        assert parse_mem_to_bytes("1024M") == 1024 * 1024 ** 2

    def test_kilobytes(self):
        assert parse_mem_to_bytes("512K") == 512 * 1024

    def test_terabytes(self):
        assert parse_mem_to_bytes("1T") == 1024 ** 4

    def test_empty(self):
        assert parse_mem_to_bytes("") == 0

    def test_null_string(self):
        assert parse_mem_to_bytes("N/A") == 0

    def test_si_mode(self):
        # sinfo Memory field: MB in SI (×1_000_000)
        assert parse_mem_to_bytes("1024", si=True) == 1024 * 1_000_000


class TestCoerceInt:
    def test_plain_int(self):
        assert coerce_int(42) == 42

    def test_zero(self):
        assert coerce_int(0) == 0

    def test_slurm24_struct_set(self):
        assert coerce_int({"set": True, "number": 5}) == 5

    def test_slurm24_struct_unset(self):
        assert coerce_int({"set": False, "number": 0}) == 0

    def test_string_int(self):
        assert coerce_int("7") == 7

    def test_none(self):
        assert coerce_int(None) == 0
