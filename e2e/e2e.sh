#!/bin/sh
set -ex
rm -rf e2e/backup e2e/restore
python -m backuppy.run --config e2e/e2e.conf backup
python -m backuppy.run --config e2e/e2e.conf restore --name e2e_backup_test --dest e2e/restore --yes
find e2e/data -type f -print0 | xargs -0 -I{} diff '{}' "e2e/restore/e2e_backup_test/$(pwd)/{}"
