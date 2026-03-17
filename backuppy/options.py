from typing import TypedDict


class OptionsDict(TypedDict):
    discard_diff_percentage: float | None
    max_manifest_versions: int | None
    skip_diff_patterns: list[str]
    use_encryption: bool
    use_compression: bool


DEFAULT_OPTIONS = OptionsDict(
    discard_diff_percentage=0.5,
    max_manifest_versions=10,
    skip_diff_patterns=[],
    use_encryption=True,
    use_compression=True,
)
