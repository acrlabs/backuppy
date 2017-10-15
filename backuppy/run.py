import sys

import yaml

from backuppy.manifest import Manifest

if __name__ == '__main__':
    with open(sys.argv[1]) as f:
        config = yaml.load(f)

    manifest = Manifest(config['directories'])
    manifest.update()
    manifest.save('/tmp/manifest')
    m2 = Manifest.load('/tmp/manifest')
