"""
This script takes the pulled tarballs from the ldeo server
and validates the checksums, decompresses the files, untars,
and sorts the packages into the proper landing zone

Last Updated: 9/21/24
"""


from datetime import date
import shutil
import os
import gzip
import subprocess
import time
import re
import math
import sqlite3


# Vars
today = date.today()
errors = []


# Guidance for landing zones and tar flags
# by data type
DATA_TYPES = {
        'Multibeam Sonar': {
            'untar': True,
            'landing_directory': '<NCEI_LANDING_SPACE>',
            'space_check': '<NCEI_LANDING_SPACE>'
        },
        'Gravimeter': {
            'untar': False,
            'landing_directory': '<NCEI_LANDING_SPACE>',
            'space_check': '<NCEI_LANDING_SPACE>'
        },
        'Magnetometer': {
            'untar': False,
            'landing_directory': '<NCEI_LANDING_SPACE>',
            'space_check': '<NCEI_LANDING_SPACE>'
        },
        'gnss': {
            'untar': False,
            'landing_directory': '<NCEI_LANDING_SPACE>',
            'space_check': '<NCEI_LANDING_SPACE>'
        },
        'r2rnav': {
            'untar': False,
            'landing_directory': '<NCEI_LANDING_SPACE>',
            'space_check': '<NCEI_LANDING_SPACE>'
        },
        'Subbottom': {
            'untar': True,
            'landing_directory': '<NCEI_LANDING_SPACE>',
            'space_check': '<NCEI_LANDING_SPACE>'
        },
        'Singlebeam Sonar': {
            'untar': True,
            'landing_directory': '<NCEI_LANDING_SPACE>',
            'space_check': '<NCEI_LANDING_SPACE>'
        },
        'Splitbeam Sonar': {
            'untar': True,
            'landing_directory': '<NCEI_LANDING_SPACE>',
            'space_check': '<NCEI_LANDING_SPACE>'
        },
        'WCD Multibeam': {
            'untar': True,
            'landing_directory': '<NCEI_LANDING_SPACE>',
            'space_check': '<NCEI_LANDING_SPACE>'
        }
    }


def convert_size(pkg_bytes):
    """
    Function to convert raw bytes to whatever
    human-readable format is closest

    Args:
        pkg_bytes(int): Number of bytes

    Returns:
         Human readable byte size

    """
    if pkg_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(pkg_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(pkg_bytes / p, 2)
    return "%s %s" % (s, size_name[i])


def get_path():
    """

    Verify landing path with user

    Args:
        User input (y/n).

    Returns(str):
        Path that exists.

    """
    # Prompt 1 & 2
    prompt = ('Use <NCEI_LANDING_SPACE> as starting path? '
              '\n[y/n] '
              '\n >>')
    prompt2 = 'Enter your preferred starting path: '

    # if y, use standard path,
    # if n, use new path
    try:
        choice = input(prompt)
        if choice == 'y':
            return '<NCEI_LANDING_SPACE>'
        elif choice == 'n':
            choice2 = input(prompt2)
            return choice2
        else:
            print(f"Input {choice} invalid. Try again. ")
    except ValueError as e:
        print(f"Input invalid. ValueError. Try again.")
        errors.append(e)


def run_subprocess(args, check=False, timeout=5000, cwd=None, number_tries=5):
    """Helper function to facilitate subprocess calls.

    Args:
        args(list): List of command components (i.e. ['ls', '-a',
                    '/path _top/dir']).
        check(bool): Control whether to use check method in subprocess. If
                     True error will be thrown when subprocess errors. If
                     false subprocess will not error on child error but error
                     message and status are passed back in "process" object.
        timeout(int): Timeout limit for subprocess in seconds. Default value
                      = 20 minutes.
        cwd(str): Path to directory to change to before executing subprocess
                  function call.
        number_tries (int): Number of tries for subprocess call before
                            returning a process object with a failed state.

    Returns(CompletedProcess): Subprocess CompletedProcess object returned
                               from subprocess.run call.

    """
    try_count = 0
    while True:
        try_count += 1
        process = subprocess.run(args=args, check=check, timeout=timeout,
                                 cwd=cwd, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 universal_newlines=True)

        if process.returncode and 'file already exists' not in process.stdout:
            if try_count >= number_tries:
                # We've tried number_tries times and we are still getting an
                # error. Give up and return process object with error.
                break
            # Wait 1 second before trying again.
            time.sleep(1)
        else:
            # The process completed properly so return process object.
            break

    # if try_count > 1:
    #     print(f'Subprocess failed {try_count} times executing {args}')

    return process


def landing_space_bytes(landing_space, minimum=1024):
    """Check the available space on landing spaces

    Use the remote server to check on space on <NCEI_LANDING_SPACE>. Return True
    if the free space is more than the minimum.

    Args:
        minimum(int): Minimum free space on <NCEI_LANDING_SPACE> specified in GB.
                        ( 1024 == 1TB)

        landing_space(str): Different team's landing zone

    Returns
        free_space(int): num of bytes of free space on landing space
        (bool): True if there is space.
        human_readable(str): num of free space in hr


    """

    # Convert minimum space to bytes.
    min_space = minimum * 1024 * 1024 * 1024
    arg_list = []

    arg_list += ('df', '-B1', '--output=avail', landing_space)

    process = run_subprocess(arg_list)
    if process.returncode:
        print(f'Unable to verify {landing_space} free space because {process.stderr}')
        return False
    else:
        raw_info = process.stdout
        try:
            space = int(re.search('\d+', raw_info).group())
            free_space = (space - min_space)
            human_readable = convert_size(int(free_space))
        except Exception as e:
            print(f'Error {e} while checking <NCEI_LANDING_SPACE> space')
            errors.append(e)
            return False
        if space >= min_space:
            print(f'The free space excluding 1TB of cushion is {human_readable}')
            return free_space, True, human_readable
        else:
            print(f'There is not enough free space on {landing_space}')
            return free_space, False, human_readable


def ungzip_tar(gzip_landing_path):
    """ Unzips the file and copies its contents to a tar file in the same 
    directory. The originally zipped file is then deleted """
    # path to gzip file .gz
    tar_landing_path = gzip_landing_path[:-3]
    with gzip.open(gzip_landing_path, 'rb') as f_in:
        with open(tar_landing_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
            # remove gzipped version
            os.remove(gzip_landing_path)
            return tar_landing_path
       

def validate_tarballs(landing_path):
    """Tar package validation engine
    Args:
        landing_path (str): System path to R2R date package tarball
            containing folder.
    Returns:
        boolean: True if tarballs are valid

    """
    response = {'file_manifest': []}
    tarball_paths = [os.path.join(landing_path, x) for x in
                     os.listdir(landing_path) if x.endswith('.tar') or x.endswith('.gz')] 

    for tarball_path in tarball_paths:
        # Check for gzip file
        if tarball_path.endswith('.gz'):
            print(f'Un-gzipping tarball {tarball_path}...')
            tarball_path = ungzip_tar(tarball_path)
    
        response['file_manifest'].append(tarball_path)
        checksum_path = tarball_path + '.md5'

        # Verify checksums for tarballs
        if os.path.isfile(checksum_path):
            with open(checksum_path) as fid:
                checksum = fid.read().split()[0]

            if not len(checksum) == 32:
                errors.append('Fail')

            print(f'calculating checksum for {tarball_path}')
            md5sum_command = ['md5sum', tarball_path]

            md5sum = run_subprocess(md5sum_command)
            if md5sum.stderr:
                print(f'Failed to compute md5 for {tarball_path}')
                errors.append('Fail')
            else:
                computed_checksum = md5sum.stdout.split()[0]
                if checksum == computed_checksum:
                    print(f'checksum valid {os.path.basename(tarball_path)}')
                else:
                    print(f'checksum validation failed; {checksum} != {computed_checksum}')
                    errors.append('Fail')

        else:
            print(f'missing checksum file for {tarball_path}')
            errors.append('Fail')

    if len(errors) > 0:
        print(response)
        return False
    else:
        return True


def database_connect(db_file):
    """Generic function to Connect to SQLITE database file.

    First check if file exists, If not throw an error. This prevents the
    sqlite3 module from creating an new blank file if it can not find the
    specified file.

    Args:
        db_file(file path): SQLite file to connect.

    Returns(db connection, cursor): Returns handle for SQLite DB connection
        and handle for cursor object.

    """
    if os.path.isfile(db_file):
        name_db = sqlite3.connect(db_file)
        name_db.row_factory = sqlite3.Row
        name_curs = name_db.cursor()
        return name_db, name_curs
    else:
        raise FileNotFoundError


def sort_landing_zone(landing_path, sqlite_file, valid=False):
    """Unpacks and organizes R2R tarballs

    Args:
        landing_path (str): System path to R2R date package tarball
            containing folder.
        sqlite_file (str): Path to R2R db file
        valid (bool): Checks and only runs if the packages are valid
    Returns:
        None
    """
    # Open the DB connection
    [db, curs] = database_connect(sqlite_file)
    if valid:
        # Find the tarballs in the dir
        tarballs = [x for x in os.listdir(landing_path) if x.endswith('.tar') or x.endswith('tar.gz')]

        # Get info about tarballs
        for tarball in tarballs:
            tarball_absolute_path = os.path.join(landing_path, tarball)
            survey, r2r_id, _ = tarball.split('_')

            # SQLite Info
            query = ('SELECT * FROM DATASETS '
                     'WHERE FILESET_ID LIKE :id')

            data = {'id': r2r_id}

            curs.execute(query, data)
            query_details = curs.fetchall()
            results = [dict(row) for row in query_details]

            # DB data from tarballs
            ship = results[0]['PLATFORM_NAME'].lower()
            data_group = results[0]['DATA_TYPE']
            data_type = results[0]['INSTRUMENT_TYPE']
            instrument_name = results[0]['INSTRUMENT_NAME']

            # Figure out which guidance using sqlite data types
            guidance = None
            # WCSD
            if data_group == 'WCSD':
                if data_type == 'Splitbeam Sonar':
                    guidance = DATA_TYPES['Splitbeam Sonar']
                else:
                    guidance = DATA_TYPES['WCD Multibeam']
            # MB
            elif data_group == 'Multibeam':
                guidance = DATA_TYPES['Multibeam Sonar']
            # Trackline still needs to be adjusted
            elif data_group == 'Trackline':
                if data_type == 'Gravimeter':
                    guidance = DATA_TYPES['Gravimeter']
                elif data_type == 'Magnetometer':
                    guidance = DATA_TYPES['Magnetometer']
                elif data_type == 'Singlebeam Sonar':
                    # Finding subbottom
                    if '[includes subbottom]' in instrument_name:
                        guidance = DATA_TYPES['Subbottom']
                    else:
                        guidance = DATA_TYPES['Singlebeam Sonar']

            if not guidance:
                print(f'tarball {tarball} data type not identified')
                continue

            # Define landing dirs
            if data_group == 'WCSD':
                tarball_landing_directory = guidance['landing_directory']
            else:
                tarball_landing_directory = guidance['landing_directory'].format(ship=ship,
                                                                                 survey=survey)
                mkdir_command = ['mkdir', '-p', tarball_landing_directory]
                mkdir = run_subprocess(mkdir_command)

                if mkdir.returncode == 0:
                    print(f'New folder created: {tarball_landing_directory}')
                else:
                    errors.append(f'Could not make new folder: {mkdir_command}')
                    continue

            # Move and untar packages based on untar flag
            if guidance['untar']:
                enough_space = landing_space_bytes(guidance['space_check'])
                if enough_space[1]:
                    print(f'There is {enough_space[2]} available in {tarball_landing_directory}')
                    tar_command = ['tar', '-C', tarball_landing_directory, '-xvf', tarball_absolute_path]

                    print(f'moving and untarring {tarball} ...')
                    tar = run_subprocess(tar_command)
                    if tar.returncode > 0:
                        print(f'issue untarring {tarball}')
                        continue
                    else:
                        # Remove the old tarballs
                        print(f'successful untaring of: {tarball}')

                        remove_command = ['rm', tarball_absolute_path]
                        rm = run_subprocess(remove_command)

                        if rm.returncode > 0:
                            print(f'issue removing {tarball}')
                        else:
                            print(f'Successful removal of {tarball}')

                        # Remove the manifest
                        remove_md5 = ['rm', f'{tarball_absolute_path}.md5']
                        rm_md5 = run_subprocess(remove_md5)

                        if rm_md5.returncode > 0:
                            print(f'issue removing {tarball}.md5')
                        else:
                            print(f'Successful removal of {tarball}.md5')

                else:
                    print(f'Not enough space in {tarball_landing_directory}')
                    continue

            # Move tarballs based on untar flag
            else:
                enough_space = landing_space_bytes(tarball_landing_directory)
                print(f'There is {enough_space[2]} available in {tarball_landing_directory}')
                if enough_space[1]:
                    rsync_command = ['rsync', '-WvlOt', tarball_absolute_path, tarball_landing_directory]
                    rsync = run_subprocess(rsync_command)

                    if rsync.returncode == 0:
                        print(f'Successful rsync of tarball {tarball}')

                        remove_command = ['rm', tarball_absolute_path]
                        rm = run_subprocess(remove_command)

                        if rm.returncode > 0:
                            print(f'issue removing {tarball}')
                        else:
                            print(f'Successful removal of {tarball}')

                    else:
                        print(f'issue rsyncing {tarball} to {tarball_landing_directory}')
                        continue
                else:
                    print(f'Not enough space in {tarball_landing_directory}')
                    continue

    db.close()
    return


def main():

    # Run space check, run validate, run untar, run sort
    landing_path = get_path()
    is_valid = validate_tarballs(landing_path)
    sort_landing_zone(landing_path, 'data/test.sqlite', is_valid)


if __name__ == '__main__':

    main()
