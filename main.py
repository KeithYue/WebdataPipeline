# coding=utf-8

# This is the main program for executing the extractor concurrently

import os
import time
import ConfigParser
import logging
from content_extractor import ExtractorFactory, BaseExtractor, udpate_weibodata
from multiprocessing import Lock, Queue, Process, cpu_count, Pool
from pymongo.errors import DuplicateKeyError

class Worker(Process):
    '''
    The worker for content extracting, each worker is assigned to one job
    '''
    def __init__(self, queue):
        Process.__init__(self)
        self.queue = queue
        return

    def run(self):
        while True:
            # get the file_path from the Q
            if not self.queue.empty():
                file_path = self.queue.get()
                extension = os.path.splitext(file_path)[-1]

                # get the extractor from unique factory
                extractor_lock.acquire()
                extractor = ExtractorFactory.get_extractor(file_path)
                extractor_lock.release()

                if extractor is not None and extension in ['.txt']: # only parse the txt file
                    print 'worker', os.getpid(), 'is parsing', file_path
                    try:
                        if extractor.parse_document():
                            extractor.insert()
                        else:
                            print 'parser error for %s, continue' % (file_path,)
                    except DuplicateKeyError:
                        print 'document existed, continue'
                        # self.queue.task_done()
                        continue # get the next document
                continue
            else:
                break
        return

def extract(file_path):
    '''
    a single task a worker would perform
    return a tuple(whehter the parsing is successful, whether there is now document has been added)
    '''
    extractor = ExtractorFactory.get_extractor(file_path)
    extension = os.path.splitext(file_path)[-1]
    if extractor is not None and extension in ['.txt']: # only parse the txt file
        print 'Parsing', file_path

        # if extractor.is_parsed():
        #     print 'This file has been parse '
        #     return False
        try:
            if extractor.parse_document():
                inc = extractor.update(ignore_parsed=True)
                return True, inc
            else:
                logging.error('Failed to parse %s, continue' % (file_path,))
                return False, 0
        except DuplicateKeyError:
            logging.error('document existed, continue...')
            return False, 0
    else:
        return False, 0

def test(func):
    '''
    using the map method to parse the file
    instead of the traditional way
    '''
    start_time = time.time()

    task_list = []
    for dirpath, dirnames, filenames in os.walk(root):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            task_list.append(file_path)

    pool = Pool(cpu_count()-2)
    results = pool.map(func, task_list)
    pool.close()
    pool.join()

    end_time = time.time()

    logging.info('Have processed %d files in %f seconds' % (len(task_list), end_time-start_time))
    logging.info('Successfully parsed %d new files' % len([ r for r in results if r[0] is True]))
    logging.info('There are %d documents has been added to the databse' % sum([r[1] for r in results]))
    return


def main():
    '''
    the execution point of the program
    '''
    start_time = time.time()
    num_of_documents = 0

    # insert the files into task Q
    Q = Queue()
    for dirpath, dirnames, filenames in os.walk(root):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            Q.put(file_path)
            num_of_documents += 1

    # set up the workers
    workers = [Worker(Q) for i in range(0, cpu_count()-2)]

    for w in workers:
        w.start()

    for w in workers:
        w.join()

    end_time = time.time()


    print 'Have processed %d files in %f seconds' % (num_of_documents, end_time-start_time)
    return


extractor_lock = Lock()

# read the config
Config = ConfigParser.ConfigParser()
Config.read('./config.ini')
root = Config.get('root', 'path')

if __name__ == '__main__':
    # set up the logging system
    logging.basicConfig(level=logging.DEBUG)
    test(extract)

