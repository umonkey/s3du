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

import json
import subprocess
import time

import boto3


class s3du(object):
    def __init__(self):
        self.s3 = boto3.client('s3')

    def list_buckets(self):
        tmp = self.s3.list_buckets()
        return [b['Name'] for b in tmp['Buckets']]

    def list_files(self):
        files = []

        buckets = self.list_buckets()
        for bucket in buckets:
            count = 0
            args = {'Bucket': bucket, 'MaxKeys': 1000}
            while True:
                res = self.s3.list_objects_v2(**args)
                for item in res['Contents']:
                    files.append(('/' + bucket + '/' + item['Key'], item['Size']))

                count += len(res['Contents'])

                if 'NextContinuationToken' in res:
                    args['ContinuationToken'] = res['NextContinuationToken']
                    print("Listing bucket %s, found %u files already..." % (bucket, count))
                else:
                    break

        return files

    def load_files(self):
        with open("s3du.json", "r") as f:
            return json.loads(f.read())

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
        files = me.list_files()
        # files = me.load_files()
        tree = me.parse_list(files)
        ncdu = me.convert_tree(tree)

        with open("ncdu.json", "w") as f:
            f.write(json.dumps(ncdu))

        subprocess.Popen(['ncdu', '-f', 'ncdu.json']).wait()


def main():
    s3du().main()


if __name__ == '__main__':
    main()
