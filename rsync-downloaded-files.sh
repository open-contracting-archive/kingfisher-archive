#!/bin/bash

# Iterate over all the collections that are fully loaded into the database

for collection in $(psql -U ocdskfpreadonly -d ocdskingfisherprocess -h localhost -c "select source_id, to_char(data_version, 'YYYYMMDD_HH24MISS') from collection where store_end_at is not null and transform_type is null" -qAtX | tr "|" "/"); do

        scraper=$(echo ${collection} | awk 'BEGIN { FS = "/" } { print $1 }')
        timestamp=$(echo ${collection} | awk 'BEGIN { FS = "/" } { print $2 }')

        # Rsync to the disk server
	ssh -o StrictHostKeyChecking=no archive@archive.kingfisher.open-contracting.org "mkdir -p /home/archive/data/${collection}"

        rsync -r -e "ssh -o StrictHostKeyChecking=no" /home/ocdskfs/scrapyd/data/${collection} archive@archive.kingfisher.open-contracting.org:/home/archive/data/${collection} || break

        # Delete original files if rsync returned 0 (restricted to the right place by sudoers)
        sudo -u ocdskfs rm -rf /home/ocdskfs/scrapyd/data/${collection}

	# Compress target files
        ssh archive@archive.kingfisher.open-contracting.org "tar -Jcvf /home/archive/data/${scraper}_${timestamp}.tar.xz /home/archive/data/$collection && rm -rf /home/archive/data/${collection}"

done;
