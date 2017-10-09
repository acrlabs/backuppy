import sys

import yaml

from backuppy.manifest import Manifest

if __name__ == '__main__':
    with open(sys.argv[1]) as f:
        config = yaml.load(f)

    for path in config['directories']:
        manifest = Manifest(path)
        manifest.update()
        manifest.save('/tmp/manifest')
        m2 = Manifest.load('/tmp/manifest')
        print(m2.path)
        print(list(m2.contents.items())[0])
