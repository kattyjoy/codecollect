from rtree.index import Rtree, CustomStorage, Property
import os
from glob import glob
import pydoop.hdfs as hdfs
import logging
import traceback
import timeit
from utils import generate_timer_log_str

logger_format = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(lineno)d: %(message)s')
logger = logging.getLogger('hdfsstorage')
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('hdfs-storage.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logger_format)
logger.addHandler(file_handler)


class HDFSStorage(CustomStorage):
    def __init__(self, file_prefix, loadexist=False, readonly=False):
        CustomStorage.__init__(self)
        if not loadexist:
            if hdfs.path.exists('{0}_0'.format(file_prefix)):
                file_prefix += '_0'
            while hdfs.path.exists('{0}_0'.format(file_prefix)):
                insert_index = file_prefix.rfind('_')
                file_prefix = '{0}_{1}'.format(file_prefix[:insert_index], int(file_prefix[insert_index + 1:]) + 1)
        self.file_prefix = file_prefix
        self.read_only = readonly
        self.clear()
        logger.info('init hdfs storage from hdfs file_prefix {0}'.format(self.file_prefix))
        try:
            total_start = timeit.default_timer()
            prefix_split = hdfs.path.splitpath(self.file_prefix)
            folder_path = prefix_split[0]
            real_prefix = prefix_split[1] + '_'
            if not hdfs.path.exists(folder_path):
                hdfs.mkdir(folder_path)

            files_info = hdfs.lsl(folder_path)
            # files_info = hdfs.lsl('{0}_*'.format(self.file_prefix))
            logger.debug('files_info:{0}'.format(files_info))
            sizecount = 0
            for file_info in files_info:
                start_time = timeit.default_timer()
                file_name = hdfs.path.splitpath(file_info['path'])[1]
                if file_name.startswith(real_prefix) and file_info['kind'] == 'file':
                    logger.debug('file info: {0}'.format(file_info))
                    page_id = file_name[len(real_prefix):]
                    if not page_id.isdigit():
                        continue
                    logger.debug('file {0} page id :{1}#'.format(file_info['path'],
                                                                 page_id))
                    # if page_id.isdigit():
                    logger.info('load {0}# page file {1}'.format(page_id,
                                                                 file_info['path']))
                    content = hdfs.load(file_info['path'], mode='r')
                    # logger.debug('{0}# page content:{1}'.format(page_id, content))
                    self.pagedict[int(page_id)] = content
                    logger.debug('{0}# page load complete'.format(page_id))
                    end_time = timeit.default_timer()
                    eval(generate_timer_log_str.format(
                        'load {0} {1} byte'.format(file_name, len(self.pagedict[int(page_id)])),
                        start_time,
                        end_time))
                    sizecount += len(self.pagedict[int(page_id)])
        except IOError, ie:
            logger.debug(traceback.format_exc())
        except Exception, e:
            logger.debug(traceback.format_exc())
        total_end = timeit.default_timer()
        eval(generate_timer_log_str.format('init hdfs storage (total page size: {0} bytes)'.format(sizecount),
                                           total_start, total_end))
        logger.info('hdfs storage init complete')

    def create(self, returnError):
        pass

    def size(self):
        total_size = 0
        for page in self.pagedict:
            total_size += len(self.pagedict[page])
        return total_size

    def destroy(self, returnError):
        if self.read_only:
            logger.debug('read only index, store nothing')
            return
        logger.debug('before destroy, store index page files to hdfs')
        total_start = timeit.default_timer()
        sizecount = 0
        for page in self.pagedict:
            start_time = timeit.default_timer()
            page_file_path = '{0}_{1}'.format(self.file_prefix, page)
            logger.info('prepare to store {0}# page named {1}'.format(page,
                                                                      page_file_path))
            with hdfs.open(page_file_path, mode='w') as output_file:
                output_file.write(self.pagedict[page])
                logger.info('store {0}# page named {1}'.format(page,
                                                               page_file_path))
            end_time = timeit.default_timer()
            eval(generate_timer_log_str.format(
                'store {0} {1} byte'.format(page_file_path, len(self.pagedict[int(page)])),
                start_time,
                end_time))
            sizecount += len(self.pagedict[page])
        total_end = timeit.default_timer()
        eval(
            generate_timer_log_str.format('store all page  (total page size: {0} bytes)'.format(sizecount), total_start,
                                          total_end))

    def clear(self):
        self.pagedict = {}

    def loadByteArray(self, page, returnError):
        try:
            return self.pagedict[page]
        except KeyError:
            returnError.contents.value = self.InvalidPageError
            logger.error('{0}# page load error, present dict {1}'.format(page, self.pagedict))

    def storeByteArray(self, page, data, returnError):
        if page == self.NewPage:
            newPageId = len(self.pagedict)
            self.pagedict[newPageId] = data
            return newPageId
        else:
            if page not in self.pagedict:
                returnError.value = self.InvalidPageError
                logger.error('{0}# page store error, present dict {1}'.format(page, self.pagedict))
                return 0
            self.pagedict[page] = data
            return page

    def deleteByteArray(self, page, returnError):
        try:
            del self.pagedict[page]
        except KeyError:
            returnError.contents.value = self.InvalidPageError
            logger.error('{0}# page delete error, present dict {1}'.format(page, self.pagedict))

    hasData = property(lambda self: bool(self.pagedict))
