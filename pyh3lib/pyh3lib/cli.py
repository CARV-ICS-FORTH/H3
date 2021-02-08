# Copyright [2019] [FORTH-ICS]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import argparse
import fnmatch
import pyh3lib

from shutil import copyfile, copytree, ignore_patterns, move, rmtree
from math import log
from datetime import datetime

# Utility functions

def sizeof(size):
    if size > 0:
        unit_list = ['bytes', 'kB', 'MB', 'GB', 'TB', 'PB']
        decimals = [0, 0, 1, 2, 2, 2]
        exponent = int(log(size, 1024))
        quotient = size / 1024**exponent
        return f'{quotient:.{decimals[exponent]}f}{unit_list[exponent]}'

    return '0 bytes'

def print_error(error):
    print(f'\033[91mERROR - {error}\033[0m')

def print_warning(warning):
    print(f'\033[93mWARNING - {warning}\033[0m')

def print_info(info):
    print(f'\033[92mINFO - {info}\033[0m')

def print_debug(args, msg):
    if args.debug:
        print(f'\033[94mDEBUG - {msg}\033[0m')

# def get_config_path():
#     try:
#         return os.environ['H3_CONFIG']
#     except KeyError:
#         config = os.path.join(os.getcwd(), 'config.ini')
#         if not os.access(config, os.F_OK):
#             config = os.path.join(os.environ['HOME'], '.h3', 'config.ini')

#         return config

def parse_h3_path(path):
    bucket = ''
    obj = ''

    if path.startswith('h3://'):
        if '/' in path[5:]:
            bucket = path[5:path.find('/',5)]
            obj = path[5+len(bucket)+1:]
        else:
            bucket = path[5:]

    return bucket, obj

def local_2_local(args, is_move):
    # Argument validation
    if args.recursive:
        if not os.path.isdir(args.src):
            return print_error(f"Invalid source folder '{args.src}'")
    else:
        if not os.path.isfile(args.src):
            return print_error(f"Invalid source file '{args.src}'")

    # Check bellow for INCLUDE pattern
    # https://stackoverflow.com/questions/52071642/python-copying-the-files-with-include-pattern
    if is_move:
        move(args.src, args.trg)
    elif args.recursive:
        copytree(args.src, args.trg, ignore=ignore_patterns(args.exclude))
    else:
        copyfile(args.src, args.trg)

def multi_filter(file_name, filters):
    for filter in filters:
        if fnmatch.fnmatch(file_name, filter):
            return True

    return False

def accept_file(file_name, args):
    if args.exclude:
        return not multi_filter(file_name, args.exclude)
    elif args.include:
#         return fnmatch.fnmatch(file_name, args.include)
        return multi_filter(file_name, args.include)
    else:
        return True

def local_2_h3(h3, args, is_move):
    trg_bucket, trg_object = parse_h3_path(args.trg)

    # Copy/Move directories
    if args.recursive:
        if not os.path.isdir(args.src):
            return print_error(f"Invalid source folder '{args.src}'")

        for dirpath, dirnames, files in os.walk(args.src):
            trimed_dirpath = dirpath[len(args.src):].strip('./')
            for file_name in files:
                full_file_name = os.path.join(dirpath, file_name)
                if accept_file(full_file_name, args) and os.path.getsize(full_file_name):

                    if trimed_dirpath:
                        path = str(trimed_dirpath + '/' + file_name)
                    else:
                        path = file_name

                    if trg_object:
                        path = str(trg_object + '/' + path).replace(' ', '_')
                    else:
                        path = str(path).replace(' ', '_')

                    try:
                        if h3.write_object_from_file(trg_bucket, path, filename=full_file_name):
                            if args.debug or not args.only_show_errors:
                                print(f'{path}')
                            if is_move:
                                os.remove(file_name)
                        else:
                            print_error(f"Failed to {('copy', 'move')[is_move]} file '{file_name}'")
                    except Exception as e:
                        # raise
                        print_error(f"Failed to {('copy', 'move')[is_move]} file '{file_name}'")
                else:
                    print_debug(args, f'Skipping {file_name}')

        if is_move:
            rmtree(args.src)

    # Copy/Move single file
    else:
        if not os.path.isfile(args.src):
            return print_error(f"Invalid source file '{args.src}'")

        if args.include or args.exclude:
            return print_error(f"Include/Exclude options not supported for individual files")

        if not trg_object:
            trg_object = args.src

        try:
            if h3.write_object_from_file(trg_bucket, trg_object, filename=args.src):
                if args.debug or not args.only_show_errors:
                    print_info(f'Uploaded file {args.src}')
                if is_move:
                    os.remove(args.src)
            else:
                print_error(f"Failed to {('copy', 'move')[is_move]} file '{args.src}'")
        except Exception as e:
            # raise
            print_error(f"Failed to {('copy', 'move')[is_move]} file '{args.src}'")

def h3_2_local(h3, args, is_move):
    src_bucket, src_object = parse_h3_path(args.src)

    # Copy/Move directories
    if args.recursive:
        if not os.path.isdir(args.trg):
            return print_error(f"Not a folder '{args.trg}'")

        for name in [x for x in h3.list_objects(src_bucket, src_object) if accept_file(x, args)]:

            # We drop the part that looks like a folder
            # full_path = os.path.join(args.trg, name[name.find('/', len(src_object))+1:])
            full_path = os.path.join(args.trg, name)
            dir_name = os.path.dirname(full_path)
            print(name, full_path, dir_name)
            try:
                os.makedirs(dir_name)
            except FileExistsError:
                pass

            try:
                h3.read_object_to_file(src_bucket, name, filename=full_path)
                if args.debug or not args.only_show_errors:
                    print_info(f'Fetched file {name}')
                if is_move:
                    h3.delete_object(src_bucket, name)
            except Exception as e:
                raise
                print_error(f'Failed to fetch file {name}')

    # Copy/Move single file
    else:
        if args.include or args.exclude:
            return print_error(f"Include/Exclude options not supported for individual files")

        if not args.trg:
            file_path = os.path.basename(src_object)
        else:
            file_path = args.trg

        try:
            h3.read_object_to_file(src_bucket, src_object, filename=file_path)
            if args.debug or not args.only_show_errors:
                print_info(f'Fetched file {file_path}')
            if is_move:
                h3.delete_object(src_bucket, src_object)
        except Exception as e:
            # raise
            print_error(f'Failed to fetch file {file_path}')

def h3_2_h3(h3, args, is_move):
    src_bucket, src_prefix = parse_h3_path(args.src)
    trg_bucket, trg_prefix = parse_h3_path(args.trg)

    if src_bucket != trg_bucket:
        print_error(f"Can not {('copy', 'move')[is_move]} across buckets")
        return

    # Copy/Rename multiple files
    if args.recursive:
        for src_object in [x for x in h3.list_objects(src_bucket, src_prefix) if accept_file(x, args)]:

            trg_object = src_object.replace(src_prefix, trg_prefix, 1)

            try:
                print(src_bucket, src_object, trg_object)
                if is_move:
                    success = h3.move_object(src_bucket, src_object, trg_object)
                else:
                    success = h3.copy_object(src_bucket, src_object, trg_object)

                if not success:
                    print_error(f"Failed to {('copy', 'move')[is_move]} '{args.src}'")
                elif args.debug or not args.only_show_errors:
                    print_info(f"{('Copied', 'Moved')[is_move]} file '{src_object}'")
            except Exception as e:
                # raise
                print_error(f"Failed to {('copy', 'move')[is_move]} '{args.src}'")

    # Copy/Rename single file
    else:
        if args.include or args.exclude:
            return print_error(f"Include/Exclude options not supported for individual files")

        try:
            if is_move:
                success = h3.move_object(src_bucket, src_prefix, trg_prefix)
            else:
                success = h3.copy_object(src_bucket, src_prefix, trg_prefix)

            if not success:
                print_error(f"Failed to {('copy', 'move')[is_move]} file '{args.src}'")
            elif args.debug or not args.only_show_errors:
                print_info(f"{('Copied', 'Moved')[is_move]} file '{args.src}'")
        except Exception as e:
            # raise
            print_error(f"Failed to {('copy', 'move')[is_move]} file '{args.src}'")

def cp_or_mv(config_path, args, is_move):
    src_bucket, src_object = parse_h3_path(args.src) #[parse_h3_path(path) for path in args.src]
    trg_bucket, trg_object = parse_h3_path(args.trg)

    print_debug(args, f'src:{args.src} -> bucket:{src_bucket} file/object:{src_object}')
    print_debug(args, f'trg:{args.trg} -> bucket:{trg_bucket} file/object:{trg_object}')

    try:
        h3 = pyh3lib.H3(config_path)

        if not src_bucket and not trg_bucket:
            local_2_local(args, is_move)
        elif not src_bucket and trg_bucket:
            local_2_h3(h3, args, is_move)
        elif src_bucket and not trg_bucket:
            h3_2_local(h3, args, is_move)
        else:
            h3_2_h3(h3, args, is_move)

    except Exception as e:
        raise
        print_error(f'Cannot {"move" if is_move else "copy"}: {e}')

#  CLI commands

def cmd_make_bucket(config_path, args):
    print_debug(args, f'command -> mb [id:{args.id}]')
    bucket, object = parse_h3_path(args.id)
    try:
        if bucket and not object:
            h3 = pyh3lib.H3(config_path)
            if h3.create_bucket(bucket):
                print_info(f'Created bucket h3://{bucket}')
            else:
                print_error(f'Failed to create bucket h3://{bucket}')
        else:
            print_error(f'Invalid bucket name {args.id}')
    except pyh3lib.H3InvalidArgsError:
        print_error(f'Invalid name')
    except pyh3lib.H3ExistsError:
        print_error(f'Bucket already exists')
    except Exception as e:
        raise
        print_error(f'Cannot create bucket: {e}')

def cmd_remove_bucket(config_path, args):
    print_debug(args, f'command -> rb [bucket:{args.id}, force:{args.force}]')
    bucket, object = parse_h3_path(args.id)
    try:
        if bucket and not object:
            h3 = pyh3lib.H3(config_path)
            if h3.list_objects(bucket):
                if not args.force:
                    print_error(f'Bucket {args.id} is not empty. Cannot delete.')
                    return
                else:
                    h3.purge_bucket(bucket)
            if h3.delete_bucket(bucket):
                print_info(f'Deleted bucket h3://{bucket}')
            else:
                print_error(f'Failed to delete bucket h3://{bucket}')
        else:
            print_error(f'Invalid bucket name {args.id}')
    except pyh3lib.H3InvalidArgsError:
        print_error(f'Invalid name')
    except pyh3lib.H3NotExistsError:
        print_error(f'Bucket does not exist')
    except Exception as e:
        raise
        print_error(f'Cannot delete bucket: {e}')

def cmd_copy(config_path, args):
    print_debug(args, f'command -> cp [src:{args.src}, trg:{args.trg}, recursive:{args.recursive}, only_errors:{args.only_show_errors}, include:{args.include},  exclude:{args.exclude}]')
    cp_or_mv(config_path, args, False)

def cmd_move(config_path, args):
    print_debug(args, f'command -> mv [src:{args.src}, trg:{args.trg}, recursive:{args.recursive}, only_errors:{args.only_show_errors}, include:{args.include},  exclude:{args.exclude}]')
    cp_or_mv(config_path, args, True)

def cmd_info(config_path, args):
    bucket, object = parse_h3_path(args.prefix)
    print_debug(args, f'command -> info [bucket:{bucket}, object:{object}')
    try:
        h3 = pyh3lib.H3(config_path)
        if not object:
            stat = h3.info_bucket(bucket, get_stats=True)
            print_info(f'Size: {stat.stats.size}')
            print_info(f'Last Modification Time: {datetime.fromtimestamp(stat.stats.last_modification).strftime("%Y-%m-%d %H:%M:%S")}')
        else:
            stat = h3.info_object(bucket, object)
            print_info(f'Size: {stat.size}')
            print_info(f'Last Modification Time: {datetime.fromtimestamp(stat.last_modification).strftime("%Y-%m-%d %H:%M:%S")}')
    except pyh3lib.H3NotExistsError:
        print_error(f'Object does not exist')
    except Exception as e:
        raise
        print_error(f'Cannot get object info: {e}')

def cmd_list(config_path, args):
    print_debug(args, f'command -> ls [prefix:{args.prefix}]')
    try:
        list_buckets = False
        if not args.prefix:
            list_buckets = True
        if not list_buckets:
            bucket, prefix = parse_h3_path(args.prefix)
            if not bucket:
                list_buckets = True

        h3 = pyh3lib.H3(config_path)
        if not list_buckets:
            for object in h3.list_objects(bucket, prefix):
                print(object)
        else:
            for bucket in h3.list_buckets():
                print(bucket)
    except pyh3lib.H3InvalidArgsError:
        print_error(f'Invalid name')
    except pyh3lib.H3NotExistsError:
        print_error(f'Bucket does not exist')
    except Exception as e:
        raise
        print_error(f'Cannot list: {e}')

def cmd_remove_object(config_path, args):
    print_debug(args, f'command -> rm [prefix:{args.prefix}, recursive:{args.recursive}, only_errors:{args.only_show_errors}]')
    try:
        h3 = pyh3lib.H3(config_path)
        bucket, object = parse_h3_path(args.prefix)

        if args.recursive:
            for item in h3.list_objects(bucket, object):
                if not h3.delete_object(bucket, item):
                    print_error(f"Failed to delete object {item}")
                elif args.debug or not args.only_show_errors:
                    print_info(f"Deleted object {item}")

        else:
            if h3.delete_object(bucket, object):
                if args.debug or not args.only_show_errors:
                    print_info(f'Deleted object {args.prefix}')
    except pyh3lib.H3InvalidArgsError:
        print_error(f'Invalid name')
    except pyh3lib.H3NotExistsError:
        print_error(f'Object does not exist')
    except Exception as e:
        raise
        print_error(f'Cannot delete object: {e}')

# def cmd_create_multipart_upload(config_path, args):
#     print_debug(args, f'command -> create_multipart_upload [bucket:{args.bucket}, key:{args.key}]')
#     try:
#         h3 = pyh3lib.H3(config_path)
#         upload_id = h3.create_multipart_upload(args.bucket, args.key)
#         if upload_id:
#             print_info(f'Upload ID: {upload_id}')
#         else:
#             print_error(f'Failed to intiate multipart upload for object h3:{args.bucket}/{args.key}')

#     except OSError as e:
#         print_error(f'Cannot access profile - {e}')
#     except RequestException:
#         print_error(f'Cannot access the H3 service at {server_url}')

# def cmd_upload_part(config_path, args):
#     print_debug(args, f'command -> upload_part [part-number:{args.part_number}, upload-id:{args.upload_id}, file:{args.file}]')
#     try:
#         h3 = pyh3lib.H3(config_path)
#         if h3.upload_part(args.file, args.upload_id, args.part_number):
#             print_info(f'Uploaded part #{args.part_number} of ID:{args.upload_id}')
#         else:
#             print_error(f'Failed to upload part #{args.part_number} of ID:{args.upload_id}')

#     except OSError as e:
#         print_error(f'Cannot access profile or {args.file} - {e}')
#     except RequestException:
#         print_error(f'Cannot access the H3 service at {server_url}')

# def cmd_upload_part_copy(config_path, args):
#     print_debug(args, f'command -> upload_part_copy [part-number:{args.part_number}, upload-id:{args.upload_id}, copy-source:{args.copy_source}]')
#     src_bucket, src_object = parse_h3_path(args.copy_source)
#     try:
#         h3 = pyh3lib.H3(config_path)
#         if h3.upload_part_copy(src_bucket, src_object, args.upload_id, args.part_number):
#             print_info(f'Uploaded part copy #{args.part_number} of ID:{args.upload_id}')
#         else:
#             print_error(f'Failed to upload part copy #{args.part_number} of ID:{args.upload_id}')

#     except OSError as e:
#         print_error(f'Cannot access profile - {e}')
#     except RequestException:
#         print_error(f'Cannot access the H3 service at {server_url}')

# def cmd_list_parts(config_path, args):
#     print_debug(args, f'command -> list_parts [upload-id:{args.upload_id}]')
#     try:
#         h3 = pyh3lib.H3(config_path)
#         parts = h3.list_parts(args.upload_id)
#         if parts:
#             print(f'Upload-ID \033[1m{args.upload_id}\033[0m bucket:\033[1m{parts.bucket}\033[0m path:\033[1m{parts.path}\033[0m')
#             for part in parts.list:
#                 print(f"    Part #{part.number}  {sizeof(part.size)}")
#         else:
#             print_error(f'Failed to retrieve the part-list for upload-id {args.upload_id}')

#     except OSError as e:
#         print_error(f'Cannot access profile - {e}')
#     except RequestException:
#         print_error(f'Cannot access the H3 service at {server_url}')

# def cmd_list_mutlipart_uploads(config_path, args):
#     print_debug(args, f'command -> list_mutlipart_uploads [bucket:{args.bucket}, prefix:{args.prefix}]')

#     legend = f'h3://{args.bucket}'
#     if args.prefix:
#         legend += f'/{args.prefix}'

#     try:
#         legend
#         h3 = pyh3lib.H3(config_path)
#         uploads = h3.list_multipart_uploads(args.bucket, args.prefix)
#         if uploads:
#             print(legend)
#             for upload in uploads:
#                 print(f"object \033[1m{upload.name}\033[0m  upload-id \033[1m{upload.id}\033[0m")
#                 for part in upload.parts:
#                     print(f"    Part #{part.number}  {sizeof(part.size)}")
#         else:
#             print_error(f'Failed to retrieve the multipart uploads for {legend}')

#     except OSError as e:
#         print_error(f'Cannot access profile - {e}')
#     except RequestException:
#         print_error(f'Cannot access the H3 service at {server_url}')

# def cmd_complete_mutlipart_upload(config_path, args):
#     print_debug(args, f'command -> complete_mutlipart_upload [upload-id:{args.upload_id}]')
#     try:
#         h3 = pyh3lib.H3(config_path)
#         if h3.complete_multipart_upload(args.upload_id):
#             print_info(f'Completed multipart upload with ID:{args.upload_id}')
#         else:
#             print_error(f'Failed to complete multipart upload with ID:{args.upload_id}')

#     except OSError as e:
#         print_error(f'Cannot access profile - {e}')
#     except RequestException:
#         print_error(f'Cannot access the H3 service at {server_url}')

# def cmd_abort_mutlipart_upload(config_path, args):
#     print_debug(args, f'command -> abort_mutlipart_upload [upload-id:{args.upload_id}]')
#     try:
#         h3 = pyh3lib.H3(config_path)
#         if h3.abort_multipart_upload(args.upload_id):
#             print_info(f'Aborted multipart upload with ID:{args.upload_id}')
#         else:
#             print_error(f'Failed to abort multipart upload with ID:{args.upload_id}')

#     except OSError as e:
#         print_error(f'Cannot access profile - {e}')
#     except RequestException:
#         print_error(f'Cannot access the H3 service at {server_url}')

def main(cmd=None):
    # config_path = get_config_path()

    parser = argparse.ArgumentParser(description='H3 command line tool')
    parser.add_argument('-d', '--debug', action='store_true', help='Print debug info')
    parser.add_argument('--version', action='version', version=f'{pyh3lib.__version__}')
    parser.add_argument('--storage', required=True, help=f'H3 storage URI')
    # parser.add_argument('--config', help=f'H3 configuration file (default: {config_path})')
    subprasers = parser.add_subparsers(dest='command', title='H3 commands')

    make_bucket = subprasers.add_parser('mb', help='Creates an H3 bucket')
    make_bucket.add_argument('id', help='An H3 compatible bucket name')
    make_bucket.set_defaults(func=cmd_make_bucket)

    remove_bucket = subprasers.add_parser('rb', help='Delete an empty H3 bucket')
    remove_bucket.add_argument('id', help='An H3 compatible bucket name')
    remove_bucket.add_argument('-f', '--force', action='store_true', help='Delete bucket along with its contents')
    remove_bucket.set_defaults(func=cmd_remove_bucket)

    # NOTE: The cp/mv operations do not support the notion of argument precedence as the aws cli does.
    #       Therefore the --exclude optional arguments have a fixed priority over the --include arguemnts.
    copy = subprasers.add_parser('cp', help='Copies a local file or H3 object to another location locally or in H3')
    copy.add_argument('src', help='Copy source') #nargs='+',
    copy.add_argument('trg', nargs='?', default='', help='Copy target')
    copy.add_argument('-r', '--recursive', action='store_true', help='Command is performed on all files or objects under the specified directory or prefix')
    copy.add_argument('-e', '--only-show-errors', action='store_true', help='Only errors and warnings are displayed. All other output is suppressed')
    copy.add_argument('--include', action='append', help= "Include files or objects in the command that match the specified pattern")
    copy.add_argument('--exclude', action='append', help= "Exclude all files or objects from the command that match the specified pattern")
    copy.set_defaults(func=cmd_copy)

    move = subprasers.add_parser('mv', help='Moves a local file or H3 object to another location locally or in H3')
    move.add_argument('src', help='Move source')
    move.add_argument('trg', nargs='?', default='', help='Move target')
    move.add_argument('-r', '--recursive', action='store_true', help='Command is performed on all files or objects under the specified directory or prefix')
    move.add_argument('-e', '--only-show-errors', action='store_true', help='Only errors and warnings are displayed. All other output is suppressed')
    move.add_argument('--include', action='append', help= "Include files or objects in the command that match the specified pattern")
    move.add_argument('--exclude', action='append', help= "Exclude all files or objects from the command that match the specified pattern")
    move.set_defaults(func=cmd_move)

    info = subprasers.add_parser('info', help='Retrieve metadata from an object without returning the object itself')
    info.add_argument('prefix', nargs='?', default=None)
    info.set_defaults(func=cmd_info)

    list = subprasers.add_parser('ls', help='List H3 objects and common prefixes under a prefix or all H3 buckets')
    list.add_argument('prefix', nargs='?', default=None)
    list.set_defaults(func=cmd_list)

    remove_object = subprasers.add_parser('rm', help='Deletes an H3 object')
    remove_object.add_argument('prefix', help='Object name or prefix')
    remove_object.add_argument('-r', '--recursive', action='store_true', help='Command is performed on all files or objects under the specified directory or prefix')
    remove_object.add_argument('-e', '--only-show-errors', action='store_true', help='Only errors and warnings are displayed. All other output is suppressed')
    remove_object.set_defaults(func=cmd_remove_object)

    # create_multipart_upload = subprasers.add_parser('create_multipart_upload', help='Initiates a multipart upload and returns an upload ID')
    # create_multipart_upload.add_argument('-b', '--bucket', help="object's bucket", required=True)
    # create_multipart_upload.add_argument('-k', '--key', help='object', required=True)
    # create_multipart_upload.set_defaults(func=cmd_create_multipart_upload)

    # upload_part = subprasers.add_parser('upload_part', help='Uploads a part in a multipart upload')
    # upload_part.add_argument('-f', '--file', help="File holding the object's data", required=True)
    # upload_part.add_argument('-p', '--part-number', type=int, help="Part number of part being uploaded. This is a positive integer between 1 and 10,000", choices=range(1, 10000), required=True)
    # upload_part.add_argument('-u', '--upload-id', help='Upload ID identifying the multipart upload whose part is being uploaded', required=True)
    # upload_part.set_defaults(func=cmd_upload_part)

    # upload_part_copy = subprasers.add_parser('upload_part_copy', help='Uploads a part by copying data from an existing object as data source')
    # upload_part_copy.add_argument('-s', '--copy-source', help="The name of the source bucket and key name of the source object, separated by a slash (/)", required=True)
    # upload_part_copy.add_argument('-p', '--part-number', type=int, help="Part number of part being uploaded. This is a positive integer between 1 and 10,000", choices=range(1, 10000), required=True)
    # upload_part_copy.add_argument('-u', '--upload-id', help='Upload ID identifying the multipart upload whose part is being uploaded', required=True)
    # upload_part_copy.set_defaults(func=cmd_upload_part_copy)

    # list_parts = subprasers.add_parser('list_parts', help='Lists the parts that have been uploaded for a specific multipart upload')
    # list_parts.add_argument('-u', '--upload-id', help='Upload ID identifying the multipart upload whose parts are to be listed', required=True)
    # list_parts.set_defaults(func=cmd_list_parts)

    # list_multipart_uploads = subprasers.add_parser('list_multipart_uploads', help='Lists in-progress multipart uploads')
    # list_multipart_uploads.add_argument('-b', '--bucket', required=True)
    # list_multipart_uploads.add_argument('-p', '--prefix', help='Lists in-progress uploads only for those keys that begin with the specified prefix')
    # list_multipart_uploads.set_defaults(func=cmd_list_mutlipart_uploads)

    # complete_multipart_upload = subprasers.add_parser('complete_multipart_upload', help='Completes a multipart upload by assembling previously uploaded parts')
    # complete_multipart_upload.add_argument('-u', '--upload-id', help='Upload ID identifying the multipart upload to be completed', required=True)
    # complete_multipart_upload.set_defaults(func=cmd_complete_mutlipart_upload)

    # abort_multipart_upload = subprasers.add_parser('abort_multipart_upload', help='Aborts a multipart upload destroying any previously uploaded parts')
    # abort_multipart_upload.add_argument('-u', '--upload-id', help='Upload ID identifying the multipart upload to be aborted', required=True)
    # abort_multipart_upload.set_defaults(func=cmd_abort_mutlipart_upload)

    args = parser.parse_args(cmd)
    # if args.config:
    #     config_path = os.path.abspath(args.config)
    config_path = args.storage # XXX Rename this...
    if args.command:
        args.func(config_path, args)
    else:
        parser.print_help(sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
