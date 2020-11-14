import logging
import os
import shutil
from collections import defaultdict

from ocdskingfisherarchive.cache import Cache
from ocdskingfisherarchive.crawl import Crawl
from ocdskingfisherarchive.s3 import S3

logger = logging.getLogger('ocdskingfisher.archive')


class Archiver:
    def __init__(self, bucket_name, data_directory, logs_directory, cache_file, cached_expired=False):
        """
        :param str bucket_name: an Amazon S3 bucket name
        :param str data_directory: Kingfisher Collect's FILES_STORE directory
        :param str logs_directory: Kingfisher Collect's project directory within Scrapyd's logs_dir directory
        :param str cache_file: the path to a SQLite database for caching the local state
        :param bool cached_expired: whether to ignore and overwrite existing rows in the SQLite database
        """
        self.s3 = S3(bucket_name)
        self.data_directory = data_directory
        self.logs_directory = logs_directory
        self.cache = Cache(cache_file, expired=cached_expired)

    def run(self, dry_run=False):
        """
        Runs the archival process.

        :param bool dry_run: whether to modify the filesystem and the bucket
        """
        # Group the crawls by remote directory.
        groups = defaultdict(list)
        for crawl in Crawl.all(self.data_directory, self.logs_directory):
            crawl = self.cache.get(crawl)

            if crawl.reject_reason:
                # Save the decision to reject the crawl.
                logger.info('Ignoring %s (%s)', crawl, crawl.reject_reason)
                crawl.archived = False
                self.cache.set(crawl)
            elif crawl.archived is False:
                logger.info('Ignoring %s', crawl)
            else:
                groups[crawl.remote_directory].append(crawl)

        for remote_directory, crawls in groups.items():
            # Add the crawl information from remote storage.
            remote = self.s3.load_exact(crawl.source_id, crawl.data_version)
            if remote:
                crawls.append(remote)

            crawls.sort(key=lambda crawl: crawl.data_version)

            # Consider each crawl in chronological order.
            best = crawls[0]
            pool = crawls[1:]

            # If no crawl is yet archived for this month, get the most recent earlier crawl.
            if not remote:
                remote = self.s3.load_latest(crawl.source_id, crawl.data_version)
                if remote:
                    best = remote
                    pool = crawls

            logger.info('%s initial (%r)', crawl, crawl.asdict())

            # Find the best crawl to archive.
            for crawl in pool:
                decision, reason = crawl.compare(best)
                if decision:
                    best = crawl
                logger.info('%s %s %s (%r)', crawl, '+' if decision else '-', reason, crawl.asdict())

            logger.info('%s final', best)
            if dry_run:
                continue

            # Mark other crawls from this month as not archived. (The crawl from an earlier month is not included.)
            if best in crawls:
                crawls.remove(best)
            for crawl in crawls:
                # Keep the crawl for 90 days.
                crawl.archived = False
                self.cache.set(best)

            # If the best crawl isn't archived, archive it. (The crawl from an earlier month is already archived.)
            if not best.archived:
                self.archive(best)
                self.cache.delete(best)

    def archive(self, crawl):
        """
        Performs the archival of the crawl.

        Creates data and metadata files, uploads them to the staging directory in the bucket, copies them to the final
        directory in the bucket, and deletes them in the staging directory.

        -  The final directory follows the pattern ``source_id/YY/MM``. As such, if a new crawl better meets the
           archival criteria than an old crawl in the same period, the old crawl's files are overwritten.
        -  The presence of a final directory indicates the crawl has already been archived. Therefore, we limit the
           risk of an incomplete upload using a staged process. (Leftover files indicate an incomplete upload.)

        Finally, it deletes the created files, the crawl's data directory, and the crawl's log file.
        """
        meta_file_name = crawl.write_meta_data_file()
        data_file_name = crawl.write_data_file()

        remote_directory = f'{crawl.source_id}/{crawl.data_version.year}/{crawl.data_version.month:02d}'

        files = {
            meta_file_name: f'{remote_directory}/metadata.json',
            data_file_name: f'{remote_directory}/data.tar.lz4',
            crawl.scrapy_log_file.name: f'{remote_directory}/scrapy.log',
        }

        for local, remote in files.items():
            self.s3.upload_file_to_staging(local, remote)
        for remote in files.values():
            self.s3.move_file_from_staging_to_real(remote)
        for remote in files.values():
            self.s3.remove_staging_file(remote)

        os.unlink(meta_file_name)
        os.unlink(data_file_name)
        shutil.rmtree(crawl.local_directory)
        crawl.scrapy_log_file.delete()

        logger.info('Archived %s', crawl)
