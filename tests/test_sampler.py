import pytest
from batchflow.sampler import SamplerConfig, apply_sampler


class TestSamplerConfig:
    def test_defaults(self):
        s = SamplerConfig()
        assert s.name == "sampler"
        assert s.rate == 1.0
        assert s.every_n is None

    def test_custom_name(self):
        s = SamplerConfig(name="my_sampler")
        assert s.name == "my_sampler"

    def test_set_rate_valid(self):
        s = SamplerConfig()
        result = s.set_rate(0.5)
        assert s.rate == 0.5
        assert result is s  # chaining

    def test_set_rate_zero(self):
        s = SamplerConfig()
        s.set_rate(0.0)
        assert s.rate == 0.0

    def test_set_rate_one(self):
        s = SamplerConfig()
        s.set_rate(1.0)
        assert s.rate == 1.0

    def test_set_rate_invalid_above(self):
        s = SamplerConfig()
        with pytest.raises(ValueError, match="rate must be between"):
            s.set_rate(1.5)

    def test_set_rate_invalid_below(self):
        s = SamplerConfig()
        with pytest.raises(ValueError, match="rate must be between"):
            s.set_rate(-0.1)

    def test_set_every_n_valid(self):
        s = SamplerConfig()
        result = s.set_every_n(3)
        assert s.every_n == 3
        assert result is s

    def test_set_every_n_invalid(self):
        s = SamplerConfig()
        with pytest.raises(ValueError, match="every_n must be >= 1"):
            s.set_every_n(0)

    def test_every_n_sampling(self):
        s = SamplerConfig().set_every_n(3)
        results = [s.should_sample(i) for i in range(9)]
        # items at positions 3, 6, 9 (1-based counter) should be True
        assert results == [False, False, True, False, False, True, False, False, True]

    def test_rate_1_always_samples(self):
        s = SamplerConfig().set_rate(1.0)
        assert all(s.should_sample(i) for i in range(20))

    def test_rate_0_never_samples(self):
        s = SamplerConfig().set_rate(0.0)
        assert not any(s.should_sample(i) for i in range(20))

    def test_seed_reproducibility(self):
        s1 = SamplerConfig().set_seed(42).set_rate(0.5)
        s2 = SamplerConfig().set_seed(42).set_rate(0.5)
        r1 = [s1.should_sample(i) for i in range(20)]
        r2 = [s2.should_sample(i) for i in range(20)]
        assert r1 == r2

    def test_reset_restores_counter(self):
        s = SamplerConfig().set_every_n(2)
        _ = [s.should_sample(i) for i in range(4)]
        s.reset()
        results = [s.should_sample(i) for i in range(4)]
        assert results == [False, True, False, True]


class TestApplySampler:
    def test_none_sampler_returns_item(self):
        assert apply_sampler(None, {"id": 1}) == {"id": 1}

    def test_sampler_passes_item(self):
        s = SamplerConfig().set_rate(1.0)
        item = {"id": 1}
        assert apply_sampler(s, item) is item

    def test_sampler_filters_item(self):
        s = SamplerConfig().set_rate(0.0)
        assert apply_sampler(s, {"id": 1}) is None
