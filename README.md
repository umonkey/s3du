# s3du

This tool uses [ncdu][1] to inspect all your S3 buckets.
Inspired by [ncdu-s3][2], which only works with a single bucket.

# Usage

```bash
$ sudo pip install s3du
$ s3du -i
```

List only files of the `STANDARD` class:

```bash
$ s3du -i -c STANDARD
```

Read [here][3] on configuring boto.


## Changes

2020-03-03:

- Fixed reading unicode file names.

[1]: http://dev.yorhel.nl/ncdu
[2]: https://github.com/EverythingMe/ncdu-s3
[3]: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
