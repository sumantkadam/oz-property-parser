#!/usr/env/bin python3

"""Main program to download NSW property sales data."""
import logging
import os
from time import time
# For Multithreading
from queue import Queue
from threading import Thread
# For Multiprocessing
# from functools import partial
from multiprocessing.pool import Pool


# from functools import partial
# from multiprocessing.pool import Pool

import download_manager

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s'
                           '- %(levelname)s - %(message)s')
LOGGER = logging.getLogger(__name__)


def get_priperty_data(download_folder: str) -> None:
    """Dowlload data from NSW property site."""
    url = R'https://valuation.property.nsw.gov.au' \
          R'/embed/propertySalesInformation'
    download_url_list = download_manager.get_download_link(url, '.zip')
    timestamp = time()
    if download_url_list:
        for download_url in download_url_list:
            download_path = download_folder + '\\' + \
                str(download_url).split('/')[-1]
            if not os.path.exists(download_path):
                try:
                    download_manager.download_file(download_url, download_path)
                except download_manager.DownloadLinkErr as dwerr:
                    LOGGER.info(F'Download error: {dwerr}')
                except FileExistsError as error:
                    LOGGER.info(F'File already exist : {error}')
            else:
                LOGGER.info(F'file already exist : {download_path}')
    else:
        LOGGER.info(F'No download link found at given url : {url}')
    logging.info(F'Took {time() - timestamp} seconds')


class DownloadWorker(Thread):
    """Download worker thead class."""

    def __init__(self, queue) -> None:
        """Construtor to initialize."""
        Thread.__init__(self)
        self.queue = queue

    def run(self) -> None:
        """Thread to run."""
        while True:
            # Get the work from the queue and expand the tuple
            link, directory = self.queue.get()
            try:
                download_manager.download_file(link, directory)
            except download_manager.DownloadLinkErr as dwerr:
                LOGGER.error(F'Download error: "{dwerr}"')
            except FileExistsError as error:
                LOGGER.warning(F'File already exist: "{error}""')
            else:
                LOGGER.info(F'Downloaded sucessfully : "{link}"')
            finally:
                self.queue.task_done()


def main_threading() -> None:
    """Threading implementations."""
    download_dir = R'C:\temp\NSWHoseSaleData\TEST'
    url = R'https://valuation.property.nsw.gov.au' \
          R'/embed/propertySalesInformation'
    queue = Queue()
    # Create 8 worker threads
    for cnt in range(8):    # pylint: disable=unused-variable
        worker = DownloadWorker(queue)
        # Setting daemon to True will let the main thread exit
        # even though the workers are blocking
        worker.daemon = True
        worker.start()
    # Put the tasks into the queue as a tuple
    download_url_list = download_manager.get_download_link(url, '.zip')
    timestamp = time()
    if download_url_list:
        for download_url in download_url_list:
            download_path = download_dir + '\\' + \
                str(download_url).split('/')[-1]
            if not os.path.exists(download_path):
                LOGGER.info(F'Queueing {download_url}')
                queue.put((download_url, download_path))
            else:
                LOGGER.info(F'file already exist : {download_path}')
    # Causes the main thread to wait for the queue to finish
    # processing all the tasks
    queue.join()
    LOGGER.info(F'Took {time() - timestamp} seconds')


def main_multiprocessing() -> None:
    """Multiprocessing implementations."""
    links = []
    filelist = []
    download_dir = R'C:\temp\NSWHoseSaleData\TEST'
    url = R'https://valuation.property.nsw.gov.au' \
          R'/embed/propertySalesInformation'
    download_url_list = download_manager.get_download_link(url, '.zip')
    if download_url_list:
        for download_url in download_url_list:
            download_path = download_dir + '\\' + \
                str(download_url).split('/')[-1]
            if not os.path.exists(download_path):
                links.append(download_url)
                filelist.append(download_path)
    # LOGGER.info(F'Took {filelist}')
    timestamp = time()
    # download = partial(download_manager.download_file, download_dir)
    with Pool(4) as process:
        process.starmap(download_manager.download_file, zip(links, filelist))

    LOGGER.info(F'Took {time() - timestamp} seconds')


if __name__ == '__main__':
    # FOLDER = R'C:\temp\NSWHoseSaleData\TEST'
    # get_priperty_data(FOLDER)
    # 2019-07-29 13:52:23,232 - root- INFO - Took 302.7314624786377 (5.05 min)
    # main_threading()
    # Queue = 4
    # 2019-07-29 14:20:07,328 - root- INFO - Took 290.4121332168579 (4.84 min)
    # Queue = 8
    # 2019-07-29 13:43:31,910 - root- INFO - Took 271.6264855861664 (4.53 min)
    # Queue = 12
    # 2019-07-29 14:26:28,334 - root- INFO - Took 284.6378810405731 (4.74 min)
    # Queue = 16
    # 2019-07-29 14:12:48,936 - root- INFO - Took 371.8655338287353 (6.20 min)
    main_multiprocessing()
    # Pool = 4
    # 2019-07-30 08:45:56,469 - __main__- INFO - Took 278.1080415248 seconds
    # Pool = 2
    # 2019-07-30 10:02:13,749 - __main__- INFO - Took 321.3222129344 seconds
    # Pool = 6
    # 2019-07-30 10:08:41,228 - __main__- INFO - Took 315.4450063705 seconds
