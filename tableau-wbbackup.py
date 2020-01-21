#!/usr/bin/env python3.6

"""
tableau-wbbackup.py  is a tool for backup and restore workbooks with access permissions

Usage:
  tableau-wbbackup.py backup <siteids>...  [-d]

Options:
  -d             Debug mode
  <siteids>...       site id
  backup         Start backup all workbooks in the site

Examples:
    tableau-wbbackup.py backup dev


"""

import logging
import tableauserverclient as TSC
import os
import sys
import json
import re

from docopt import docopt
from logging.handlers import TimedRotatingFileHandler


class BackupTableauSite(object):
    def __init__(self, server, username, password, backup_dir, site_id=''):
        self.logger = logging.getLogger('main.BackupTableauSite')
        self.tsc_server = TSC.Server(server_address=server, use_server_version=True)
        tsc_auth = TSC.TableauAuth(username=username, password=password,site_id=site_id)
        self.logger.debug(f"Login to {server}, siteid: {site_id}")
        self.tsc_server.auth.sign_in(tsc_auth)
        self.site_id = site_id
        self.backup_dir = backup_dir

    def _populate_wg_pr(self):
        self.logger.debug("Run __populate_wg_pr")
        request_options = TSC.RequestOptions(pagesize=25)
        self.all_workbooks = list(TSC.Pager(self.tsc_server.workbooks, request_options))
        self.logger.debug(f"all_workbooks len {len(self.all_workbooks)}")
        self.all_projects = list(TSC.Pager(self.tsc_server.projects, request_options))
        self.logger.debug(f"all_projects len {len(self.all_projects)}")

    def _save_params(self, tbl_object, path, filename=''):
        permissions_list = [
            {'grantee': {'id': p.grantee.id, 'tag_name': p.grantee.tag_name}, 'capabilities': p.capabilities} for p in
            tbl_object.permissions]

        params = {"name": tbl_object.name, "filename": filename, "permissions": permissions_list }
        self.logger.debug(f"_save_params: {params} to {path}")
        with open(path, 'w') as f:
            json.dump(params, f)

    def _backup_projects(self):
        for project in self.all_projects:
            self._download_project(project, os.path.join(self.backup_dir, self.site_id))
        for wb in self.all_workbooks:
            self._download_workbook(wb, os.path.join(self.backup_dir, self.site_id))

    def _parent_id_to_path(self, parent_id):
        parent_project = [p for p in self.all_projects if p.id == parent_id].pop()
        path = parent_project.name
        if parent_project.parent_id:
            path = os.path.join(self._parent_id_to_path(parent_project.parent_id), path)
        return path

    def _download_workbook(self, workbook, base_dir):
        self.logger.debug(f"Run __download_workbook : {workbook.name}")
        base_dir = os.path.join(base_dir, self._parent_id_to_path(workbook.project_id))
        self.tsc_server.workbooks.populate_permissions(workbook)
        base_dir = os.path.join(base_dir, 'workbooks')
        self._create_folder(base_dir)
        permissions_file_path = os.path.join(base_dir, f'{self._remove_bad_path_characters(workbook.name)}.json')
        wb_path = self.tsc_server.workbooks.download(workbook.id, filepath=base_dir, no_extract=True)
        _, filename = os.path.split(wb_path)
        self.logger.debug(f"Workbook was downloaded to {wb_path}")
        self._save_params(workbook, permissions_file_path, filename)

    def _download_project(self, project, base_dir):
        self.logger.debug(f"Run __download_project : {project.name}")
        if project.parent_id:
            base_dir = os.path.join(base_dir, self._parent_id_to_path(project.parent_id))
        project_filename = self._remove_bad_path_characters(project.name)
        project_dir = os.path.join(base_dir, project_filename)
        self._create_folder(project_dir)
        self.tsc_server.projects.populate_permissions(project)
        permissions_file_path = os.path.join(project_dir, f'{project_filename}.json')
        self._save_params(project, permissions_file_path)

    def _create_folder(self, folder_path):
        if not os.path.exists(folder_path):
            self.logger.debug(f"Create folder: {folder_path}")
            os.makedirs(folder_path)

    def _remove_bad_path_characters(self, filename):
        new_filename = re.sub('[^\w\-_\. ]', '_', filename)
        if new_filename != filename:
            self.logger.debug(f"_remove_bad_path_characters: {filename} ->  {new_filename}")
        return new_filename

    def run_backup(self):
        self.logger.debug("Run run_backup")
        self._populate_wg_pr()
        self._backup_projects()


def main():
    logger = logging.getLogger('main')

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

    if argz.get('backup'):
        logger.info(f"Run backup {argz.get('<siteids>')}")
        for site_id in argz.get('<siteids>'):
            bts = BackupTableauSite(server=config.get('server'),
                                    username=config.get('user'),
                                    password=config.get('password'),
                                    backup_dir=config.get('backup_dir'),
                                    site_id=site_id)
            bts.run_backup()

if __name__ == '__main__':
    main()
