import re
import os
import configparser
import logging

config = configparser.ConfigParser()
config.read('config_FileRename.ini', encoding='utf-8')

logfile = config['DATA_MIGRATION_FILE_RENAME']['logfile']
basedirectory = config['DATA_MIGRATION_FILE_RENAME']['basedirectory']

logging.basicConfig(filename=logfile, format='%(asctime)s %(levelname)s %(message)s', filemode='w')
logger = logging.getLogger()
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)

logger.info("Renaming files inside Directory: " + basedirectory)
count = 0
totalcount = 0
logger.info(" Rename Service has been started.....")
for path, subdirs, files in os.walk(basedirectory):
    curcount = 0
    if 'manifest' in path:
        logger.info(path + ": No renaming!")
    else:
        for filename in files:
            # print filename
            srcfilename = os.path.join(path, filename)
            filename = re.sub(r'[^a-zA-Z0-9._-]', "", filename)
            destfilename = os.path.join(path, filename)
            totalcount += 1
            if srcfilename != destfilename:
                try:
                    os.rename(srcfilename, destfilename)
                    logger.info(" File renamed from [" + srcfilename + "]  to [" + destfilename + "]")
                    curcount += 1
                except Exception as e:
                    logger.error(str(e))
                    logger.error(" Issue while renaming file: " + str(srcfilename))
        count += curcount
logger.info("Total number of file renamed: " + str(count))
logger.info("Total number of file scanned: " + str(totalcount))
logger.info("Rename Service has been completed successfully!")
