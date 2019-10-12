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
-i   interactive, run ncdu
-v   verbose, display some progress"""


class s3du(object):
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.verbose = False
        self.interactive = False
        self.filename = None
        self.keep_file = False

    def usage(self):
        print(USAGE, file=sys.stderr)
        exit(1)

    def parse_args(self, argv):
        if len(argv) == 1:
            self.usage()

        for arg in argv[1:]:
            if arg == '-i':
                self.interactive = True
            elif arg == '-v':
                self.verbose = True
            elif arg.startswith('-'):
                self.usage()
            elif self.filename:
                self.usage()
            else:
                self.filename = arg
                self.keep_file = True

        if not self.filename:
            warnings.filter('ignore', 'tempnam')
            self.filename = os.tempnam(tempfile.gettempdir(), 's3du_')

    def list_buckets(self):
        tmp = self.s3.list_buckets()
        return [b['Name'] for b in tmp['Buckets']]

    def list_files(self):
        files = []

        buckets = self.list_buckets()
        for bucket in buckets:
            if self.verbose:
                print("Listing bucket %s, found %u files already..." % (bucket, len(files)))

            args = {'Bucket': bucket, 'MaxKeys': 1000}
            while True:
                res = self.s3.list_objects_v2(**args)
                for item in res['Contents']:
                    files.append(('/' + bucket + '/' + item['Key'], item['Size']))

                if 'NextContinuationToken' in res:
                    args['ContinuationToken'] = res['NextContinuationToken']
                    if self.verbose:
                        print("Listing bucket %s, found %u files already..." % (bucket, len(files)))
                else:
                    break

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

        files = me.list_files()
        tree = me.parse_list(files)
        ncdu = me.convert_tree(tree)

        with open(me.filename, "w") as f:
            f.write(json.dumps(ncdu))

        subprocess.Popen(['ncdu', '-f', me.filename]).wait()

        if not me.keep_file:
            os.unlink(me.filenam)


def main():
    s3du().main()


if __name__ == '__main__':
    main()
