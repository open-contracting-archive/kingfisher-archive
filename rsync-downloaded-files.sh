#!/bin/bash

set -e

# Iterate over all the collections that are fully loaded into the database

for collection in $(psql -U ocdskfpreadonly -d ocdskingfisherprocess -h localhost -c "select source_id, to_char(data_version, 'YYYYMMDD_HH24MISS') from collection where store_end_at is not null and transform_type is null" -qAtX | tr "|" "/"); do

	# Run date so that when output appears in log we know when it happened
	date

        echo "COLLECTION ${collection}"

        scraper=$(echo ${collection} | awk 'BEGIN { FS = "/" } { print $1 }')
        timestamp=$(echo ${collection} | awk 'BEGIN { FS = "/" } { print $2 }')


        # Find folder on source server
	# LOCAL_DIR serves 2 purposes; the data may be in one of 2 places AND more importantly, if the data has already been successfully archived, nothing is done.
	LOCAL_DIR=""
	if [ -d "/home/ocdskfs/scrapyd/data/${collection}/" ]; then 
		LOCAL_DIR="/home/ocdskfs/scrapyd/data/${collection}/"
	elif [ -d "/home/ocdskfs/old-data/${collection}" ]; then 
		LOCAL_DIR="/home/ocdskfs/old-data/${collection}/"
	fi
	
        if [ "${LOCAL_DIR}" ]; then

		echo "Found ${LOCAL_DIR}"

		# Create folder on disk server
		echo "Create folder on disk server"
		ssh -o StrictHostKeyChecking=no archive@archive.kingfisher.open-contracting.org "mkdir -p /home/archive/data/${collection}"

		# Rsync to the disk server
		echo "Rsync to the disk server"
		rsync -r -e "ssh -o StrictHostKeyChecking=no" ${LOCAL_DIR} archive@archive.kingfisher.open-contracting.org:/home/archive/data/${collection}/ || continue

		# Delete original files if rsync returned 0 (restricted to the right place by sudoers)
		echo "Delete original files"
		sudo -u ocdskfs rm -rf ${LOCAL_DIR}

		# Compress target files
		echo "Compress target files"
		ssh archive@archive.kingfisher.open-contracting.org "tar -Jcvf /home/archive/data/${scraper}_${timestamp}.tar.xz /home/archive/data/$collection && rm -rf /home/archive/data/${collection}"

		########### TEMP TEMP TEMP while this is still new and in testing, we only transfer one at a time and then stop
		exit 1

	fi

done;

