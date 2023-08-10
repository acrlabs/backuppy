import staticconf


def setup_config(config_file: str) -> None:
    staticconf.YamlConfiguration(config_file, flatten=False)
    for backup_name, backup_config in staticconf.read('backups').items():  # type: ignore[attr-defined]
        staticconf.DictConfiguration(backup_config, namespace=backup_name)
