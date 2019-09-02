from typing import Optional

from mypy_extensions import TypedDict


class OptionsDict(TypedDict):
    max_manifest_versions: Optional[int]
    use_encryption: bool
    use_compression: bool


DEFAULT_OPTIONS = OptionsDict(
    max_manifest_versions=10,
    use_encryption=True,
    use_compression=True,
)
