from mypy_extensions import TypedDict


class OptionsDict(TypedDict):
    use_encryption: bool
    use_compression: bool


DEFAULT_OPTIONS = OptionsDict(
    use_encryption=True,
    use_compression=True,
)
