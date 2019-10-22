#!/bin/sh
set -ex
rm -rf e2e/backup e2e/restore
if [ ! -f e2e/data/big ]; then
    dd if=/dev/urandom of=./e2e/data/big bs=10M count=200
fi
time python -m backuppy.run --log-level debug2 --config e2e/e2e.conf backup
time python -m backuppy.run --log-level debug2 --config e2e/e2e.conf restore --name e2e_backup_test --dest e2e/restore --yes
find e2e/data -type f -print0 | xargs -0 -I{} diff '{}' "e2e/restore/e2e_backup_test/$(pwd)/{}"
