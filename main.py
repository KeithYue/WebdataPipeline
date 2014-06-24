# coding=utf-8

# This is the main program for executing the extractor concurrently

import os
import time
import ConfigParser
from content_extractor import ExtractorFactory
from multiprocessing import Lock, Queue, Process, cpu_count
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


def main():
    '''
    the execution point of the program
    '''
    start_time = time.time()
    num_of_documents = 0

    # read the config
    Config = ConfigParser.ConfigParser()
    Config.read('./config.ini')
    root = Config.get('root', 'path')

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

if __name__ == '__main__':
    main()

