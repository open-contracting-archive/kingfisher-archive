#!/bin/bash
set -e

# Iterate over all the collections that are fully loaded into the database

for collection in $(psql -U ocdskfpreadonly -d ocdskingfisherprocess -h localhost -c "SELECT source_id, to_char(data_version, 'YYYYMMDD_HH24MISS') FROM collection WHERE store_end_at IS NOT NULL AND transform_type = ''" -qAtX | tr "|" "/"); do
  # If the data has already been successfully archived, nothing is done.
  if [ -d "/home/ocdskfs/scrapyd/data/${collection}/" ]; then 
	  date -u
	  echo "COLLECTION ${collection}"

    LOCAL_DIR="/home/ocdskfs/scrapyd/data/${collection}/"
    echo "Found ${LOCAL_DIR}"

    echo "Create folder on disk server"
    ssh archive@archive.kingfisher.open-contracting.org "mkdir -p /home/archive/data/${collection}"

    echo "Rsync to the disk server"
    rsync -avz ${LOCAL_DIR} archive@archive.kingfisher.open-contracting.org:/home/archive/data/${collection}/ || continue

    # Delete original files if rsync returned 0 (restricted to the right place by sudoers)
    echo "Delete original files"
    sudo -u ocdskfs rm -rf ${LOCAL_DIR}

    echo "Compress target files"
    ssh archive@archive.kingfisher.open-contracting.org "cd /home/archive/data && tar cf - $collection | lz4 - $(echo "$collection" | tr "/" "_").tar.lz4 && rm -rf $collection && find . -type d -mindepth 1 -empty -delete"
  fi
done
