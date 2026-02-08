import pytest

from datasentinel.strategy_factory import StrategyFactory
from datasentinel.comparison_strategy import FullOuterJoinStrategy


def test_get_comparison_strategy_default():
    strategy = StrategyFactory.get_comparison_strategy({})
    assert isinstance(strategy, FullOuterJoinStrategy)


def test_get_comparison_strategy_unknown():
    with pytest.raises(ValueError, match="Unknown comparison strategy"):
        StrategyFactory.get_comparison_strategy({"comparison_type": "bogus"})
