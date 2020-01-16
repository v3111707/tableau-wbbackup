#!/usr/bin/env python3.6

"""
tableau-wbbackup.py  is a tool for backup and restore workbooks with access permissions

Usage:
  tableau-wbbackup.py backup <siteid>  [-d]

Options:
  -d             Debug mode
  <siteid>       site id
  backup         Start backup all workbooks in the site

Examples:
    tableau-wbbackup.py backup dev


"""

import logging
import tableauserverclient as TSC
import os
import sys
import json

from docopt import docopt
from logging.handlers import TimedRotatingFileHandler

def create_folder(folder_path):
    if not os.path.isdir(folder_path):
        os.mkdir(folder_path)


def main():
    logger = logging.getLogger('tableau-granularbackup.py')

    if '-d' in sys.argv:
        print(f"argv: {sys.argv}")
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.json')
    try:
        config_data = open(config_path)
    except Exception as e:
        logger.error(f"Error while reading from \"{config_path}\": {e}")
        sys.exit(1)
    try:
        config = json.load(config_data)
    except Exception as e:
        logger.error(f"Error while parsing \"{config_path}\": {e}")
        sys.exit(1)
    logger.debug(f"\"{config_path}\" was loaded")

    if os.path.isabs(config.get('logfile_path')):
        logfile_path = config.get('logfile_path')
    else:
        logfile_path = os.path.abspath(config.get('logfile_path'))
    logger.debug(f"logfile_path: \"{logfile_path}\"")

    fh = TimedRotatingFileHandler(logfile_path, when="W0", interval=1, backupCount=1)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.debug('Start')
    argz = docopt(__doc__, argv=sys.argv[1:])
    logger.debug(f'argz: {argz}')

    tserver = TSC.Server(server_address=config.get('server'))
    tauth = TSC.TableauAuth(username=config.get('user'), password=config.get('password'), site_id=argz.get('<siteid>'))
    backup_dir = config.get('backup_dir')
    logger.debug(f'backup_dir: \"{backup_dir}\"')

    with tserver.auth.sign_in(tauth):
        request_options = TSC.RequestOptions(pagesize=25)
        all_workbooks = list(TSC.Pager(tserver.workbooks, request_options))
        all_projects = list(TSC.Pager(tserver.projects, request_options))
        site_dir = os.path.join(backup_dir,tauth.site_id)

        create_folder(site_dir)

        for wb in all_workbooks:
            project_dir = os.path.join(site_dir,wb.project_name)
            create_folder(project_dir)
            wb_dir = os.path.join(project_dir, wb.name)
            create_folder(wb_dir)
            tserver.workbooks.download(wb.id, filepath=wb_dir)
            tserver.workbooks.populate_permissions(wb)


            print(wb)

    print('END')
if __name__ == '__main__':
    main()
