from pathlib import Path

from ruamel.yaml import YAML

from fresnel import constants

yaml = YAML(typ='rt')


class ConfigNamespace:
    def __init__(self, filepath: Path, args_namespace=None):
        self.args = args_namespace

        if not filepath.exists():
            filepath.touch(constants.FILE_MODE)

        self.config_filepath = filepath
        self.load_config()

    def __getitem__(self, key):
        value = getattr(self.args, key, None)
        if value is not None:
            return value
        return self.cfg[key]

    def __setitem__(self, key, value):
        if hasattr(self.args, key):
            delattr(self.args, key)
        self.cfg[key] = value
        self.save_config()

    def __delitem__(self, key):
        if hasattr(self.args, key):
            delattr(self.args, key)
        del self.cfg[key]
        self.save_config()

    def __contains__(self, item):
        return hasattr(self.args, item) or (item in self.cfg)

    def get(self, key, default=None, comment=None):
        try:
            return self[key]
        except KeyError:
            self[key] = default
            if comment:
                self.comment(key, comment)
            return default

    def comment(self, key, text):
        self.cfg.yaml_set_comment_before_after_key(
            key, before=text)
        self.save_config()

    def load_config(self):
        self.cfg = yaml.load(self.config_filepath)
        if self.cfg is None:
            self.cfg = {}

    def save_config(self):
        yaml.dump(self.cfg, self.config_filepath)
        self.load_config()
