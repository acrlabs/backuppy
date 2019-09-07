from typing import List
from typing import Optional

from mypy_extensions import TypedDict


class OptionsDict(TypedDict):
    discard_diff_percentage: Optional[float]
    max_manifest_versions: Optional[int]
    skip_diff_patterns: List[str]
    use_encryption: bool
    use_compression: bool


DEFAULT_OPTIONS = OptionsDict(
    discard_diff_percentage=0.5,
    max_manifest_versions=10,
    skip_diff_patterns=[],
    use_encryption=True,
    use_compression=True,
)
