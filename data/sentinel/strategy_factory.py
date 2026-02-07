# strategy_factory.py

from data.sentinel.comparison_strategy import (
    FullOuterJoinStrategy,
    ComparisonStrategy,
)


class StrategyFactory:
    @staticmethod
    def get_comparison_strategy(config: dict) -> ComparisonStrategy:
        """
        Determines the comparison strategy based on the configuration.
        """
        strategy_name = config.get("comparison_type", "full_outer_join")

        if strategy_name == "full_outer_join":
            return FullOuterJoinStrategy()
        else:
            raise ValueError(
                f"Unknown comparison strategy: {strategy_name}"
            )


if __name__ == "__main__":
    # example usage
    pass