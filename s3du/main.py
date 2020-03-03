#!/usr/bin/env python3
# vim: set fileencoding=utf-8 tw=0:
#
# Uses ncdu to interactively inspect all available S3 buckets.
#
# https://github.com/umonkey/s3du
#
# TODO:
# - use system tempdir to store the intermediate json file
# - make sure ncdu is installed
# - make sure boto is configured

from __future__ import print_function

import csv
import json
import os
import subprocess
import sys
import tempfile
import time
import warnings

import boto3


USAGE = """Usage: s3du options|filename.json

Options:
-i      interactive, run ncdu
-v      verbose, display some progress
-c STR  only show this storage class, values: STANDARD, STANDARD_IA, GLACIER, DEEP_ARCHIVE"""


class s3du(object):
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.verbose = False
        self.interactive = False
        self.filename = None
        self.keep_file = False
        self.storage_class = None
        self.classes = set()
        self.csv_name = os.path.expanduser('~/.cache/s3du-cache.csv')

    def usage(self):
        print(USAGE, file=sys.stderr)
        exit(1)

    def parse_args(self, argv):
        if len(argv) == 1:
            self.usage()

        prev = None
        for arg in argv[1:]:
            if prev == '-c':
                self.storage_class = arg
                prev = None
                continue
            if arg == '-i':
                self.interactive = True
            elif arg == '-v':
                self.verbose = True
            elif arg == '-c':
                prev = arg
                continue
            elif arg.startswith('-'):
                self.usage()
            elif self.filename:
                self.usage()
            else:
                self.filename = arg
                self.keep_file = True

        if not self.filename:
            warnings.simplefilter('ignore', 'tempnam')
            self.filename = tempfile.mkstemp(dir=tempfile.gettempdir(), prefix='s3du_')[1]

    def list_buckets(self):
        tmp = self.s3.list_buckets()
        return [b['Name'] for b in tmp['Buckets']]

    def cache_files(self):
        if os.path.exists(self.csv_name):
            if time.time() - os.stat(self.csv_name).st_mtime < 3600:
                print('Using file list from cache: %s' % self.csv_name)
                return

        with open(self.csv_name, 'w') as f:
            writer = csv.writer(f)

            buckets = self.list_buckets()
            for bucket in buckets:
                if self.verbose:
                    print("Listing bucket %s, found %u files already..." % (bucket, len(files)))

                args = {'Bucket': bucket, 'MaxKeys': 1000}
                while True:
                    res = self.s3.list_objects_v2(**args)
                    for item in res['Contents']:
                        writer.writerow([bucket, item['Key'].encode('utf-8'), item['Size'], item['StorageClass']])

                    if 'NextContinuationToken' in res:
                        args['ContinuationToken'] = res['NextContinuationToken']
                        if self.verbose:
                            print("Listing bucket %s, found %u files already..." % (bucket, len(files)))
                    else:
                        break

    def list_files(self):
        """Lists all files and saves then in the cache file."""
        files = []

        with open(self.csv_name, 'r') as f:
            reader = csv.reader(f)
            for (bucket, key, size, sclass) in reader:
                if self.storage_class and self.storage_class != sclass:
                    continue

                path = '/' + bucket + '/' + key
                size = int(size)
                files.append((path, size))

        return files

    def parse_list(self, files):
        tree = {'dirs': {}, 'files': []}

        for (path, size) in files:
            path = path.split("/")
            fname = path.pop()

            r = tree
            for em in path:
                if em not in r['dirs']:
                    r['dirs'][em] = {'dirs': {}, 'files': []}
                r = r['dirs'][em]

            r['files'].append((fname, size))

        return tree

    def convert_tree(self, tree):
        ncdu = [1, 0, {
            "timestamp": int(time.time()),
            "progver": "0.1",
            "progname": "s3du",
        }]

        ncdu.append(self.convert_branch(tree['dirs'][''], 'S3'))
        return ncdu

    def convert_branch(self, branch, name):
        res = []
        res.append({'name': name or '(unnamed)'})

        for k, v in branch['dirs'].items():
            res.append(self.convert_branch(v, k))

        for (fname, size) in branch['files']:
            res.append({
                'name': fname or '(unnamed)',
                'dsize': size,
            })

        return res

    @classmethod
    def main(cls):
        me = cls()
        me.parse_args(sys.argv)

        me.cache_files()  # list files and write ~/.cache/s3du-cache.csv

        files = me.list_files()
        tree = me.parse_list(files)
        ncdu = me.convert_tree(tree)

        with open(me.filename, "w") as f:
            f.write(json.dumps(ncdu))

        subprocess.Popen(['ncdu', '-f', me.filename]).wait()

        if not me.keep_file:
            os.unlink(me.filename)

        print('Found objects of classes: %s.' % ', '.join(me.classes))


def main():
    s3du().main()


if __name__ == '__main__':
    main()
