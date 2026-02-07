import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PathResolver:
    sentinel_home: str | None

    @classmethod
    def from_env(cls) -> "PathResolver":
        sentinel_home = os.getenv("SENTINEL_HOME")
        if sentinel_home:
            sentinel_home = os.path.abspath(os.path.expanduser(sentinel_home))
        return cls(sentinel_home=sentinel_home)

    def resolve(self, path: str | None, *, allow_cwd_fallback: bool = False) -> str | None:
        if not path:
            return path
        if os.path.isabs(path):
            return path
        if self.sentinel_home:
            return os.path.join(self.sentinel_home, path)
        if allow_cwd_fallback:
            return os.path.abspath(path)
        raise ValueError(
            "Relative path provided but SENTINEL_HOME is not set. "
            "Set SENTINEL_HOME or use an absolute path."
        )

    def base_dir(self) -> str:
        if self.sentinel_home:
            return self.sentinel_home
        return os.getcwd()
