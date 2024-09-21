"""
This script connects to the ldeo sftp server and queries the landing space for
R2R/NCEI datasets and updates a SQLite database to create an inventory.
It then queries the SQLite db for the selected data types and copies
packages to the landing space given the amount of free space.
"""

import time
import math
import re
import os
import sqlite3
import requests
from datetime import datetime
import subprocess
import platform


# SQLite file
sqlite_file = 'data/r2r_master_inventory.sqlite'
errors = []
operating_system = platform.system()

# Server Credentials
try:
    ldeo_creds = os.path.join(
                os.path.expanduser("~"), '.connections', 'r2r_creds.txt')
    with open(ldeo_creds, 'r') as f:  # Read in
        # credentials
        creds = []
        for line in f:
            line = line.rstrip()
            creds.append(line)
        port_num = creds[0]
        r2r_server_path = creds[1]
        ngdc_files_path = creds[2]

except Exception as e:
    print(f'Error {e} getting server credentials')


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


def landing_space_bytes(ncei_landing_space, minimum=1024):
    """Check the available space on <NCEI Landing Space>.

    Use the remote server to check on space on <NCEI Landing Space>. Return True
    if the free space is more than the minimum.

    Args:
        ncei_landing_space(str): Path to landing space
        minimum(int): Minimum free space on r2r_landing specified in GB.
                        ( 1024 == 1TB)

    Returns
        free_space(int): num of bytes of free space on landing space
        (bool): True if there is free space.


    """

    # Convert minimum space to bytes.
    min_space = minimum * 1024 * 1024 * 1024
    arg_list = []

    arg_list += ('df', '-B1', '--output=avail', ncei_landing_space)

    process = run_subprocess(arg_list)
    if process.returncode:
        print(f'Unable to verify <NCEI Landing Space> free space because {process.stderr}')
        return False
    else:
        raw_info = process.stdout
        try:
            space = int(re.search('\d+', raw_info).group())
            free_space = (space - min_space)
            human_readable = convert_size(int(free_space))
        except Exception as e:
            print(f'Error {e} while checking <NCEI Landing Space>')
            errors.append(e)
            return False
        if space >= min_space:
            print(f'The free space excluding 1TB of cushion is {human_readable}')
            return free_space, True
        print('There is not enough free space on <NCEI Landing Space>')
        return free_space, False


def connect_to_sftp(machine, port, sftp_server, file_path):
    """
    This function connects to th ldeo sftp server and creates
    a list of directories on the server

     Args:
        machine(str): platform.system() to support testing and regular runs
        port(str): proper port to access server
        sftp_server: name and user for the r2r sftp server
        file_path: cd command into the right directory

    Returns:
        r2r_dates(list): List of date dirs found on r2r server
    """

    # Test credentials
    hostname = '127.0.0.1'
    test_port_num = '2222'
    username = 'testuser'
    private_key_file = 'ssh/ssh_host_rsa_key'

    # Initialize list
    r2r_dates = []

    # CLI command and subprocess
    try:

        if machine != 'Darwin' and machine != 'Windows':
            process = subprocess.run(['sftp', '-P', port,
                                      sftp_server], input=f'ls -l {file_path}\n',
                                     text=True, capture_output=True)
        else:
            process = subprocess.run(['sftp', '-i', private_key_file, '-P', test_port_num,
                                      f'{username}@{hostname}'], input=f'ls -l /data\n',
                                     text=True, capture_output=True)

        if process.returncode:
            if 'matched no objects' in process.stderr:
                # 0 means success
                return 0
            else:
                print(f'Error in  {sftp_server}')
                errors.append(f'Error in  {sftp_server}')
                print(process.stderr)

        # Format output
        else:
            # Splits each line of return and
            # also excludes the command print
            return_list = process.stdout.split('\n')[1:]

            print("Gathering new directories ...")
            # Parse through output list
            for line in return_list:
                parts = line.split()  # check if the line is empty
                if parts:
                    date_str = parts[-1]
                    r2r_dates.append(date_str)
            # Get rid of "README" file
            r2r_dates.remove('README')

            return r2r_dates

    # Error trap
    except subprocess.CalledProcessError as e:
        print('Error in "Connect to SFTP":', e)
        print("Standard Error: ", e.stderr)
        raise RuntimeError(e)


def check_date_dirs(date_list, file_path, db_file):

    """
    This function moves through the list of date directories and
    reformats them as datetime objects, parses for the right times, and then
    adds all the information into a dictionary for parsing

    Args:
        date_list (list): list of date directories found on server
        file_path (str): Path to ngdc data on r2r server
        db_file (str): Sqlite file with r2r_inventory

    Returns:
        date_dir_inventory (nested dict): dict with date dirs as keys
        values are list of
            - r2r_id (int): 5 digit r2r fileset_id
            - package_path (str): sftp path to file for copying
    """

    # New package inventory to add to DB
    # Allows the user to access path, package names, and r2r_id all in the same place
    date_dir_inventory = {}
    # Dates in the database
    db_date_list = []
    # How many packages were added?
    new_dir_count = 0

    try:
        # Connect to SQLite file
        [db, curs] = database_connect(db_file)
        try:
            curs.execute('select DATE_DIR from "DATASETS"')
            details = curs.fetchall()

            if not details:
                print('SQLite file is empty')
                details_list = []
            else:
                details_list = [dict(row) for row in details]

                # Access dates in DB
                for i in details_list:
                    dates = list(i.values())[0]
                    if dates not in db_date_list:
                        db_date_list.append(dates)
                    else:
                        # Avoid repeat dates
                        continue

            print('Creating inventory ...')
            # Compare dates on server and DB
            for date in date_list:
                if date not in db_date_list:
                    dt = datetime.strptime(date, "%Y-%m-%d")
                    if dt > datetime(2021, 1, 1):

                        # Test case vs Real Run
                        if platform.system() != 'Darwin' and platform.system() != 'Windows':
                            date_dir = f'{file_path}/{date}'
                            process = subprocess.run(['sftp', '-P', port_num,
                                                      r2r_server_path], input=f'ls -l {date_dir}\n',
                                                     text=True, capture_output=True)
                        else:
                            date_dir = f'/data/{date}'
                            process = subprocess.run(['sftp', '-i', 'ssh/ssh_host_rsa_key', '-P', '2222',
                                                      'testuser@127.0.0.1'], input=f'ls -l {date_dir}\n',
                                                     text=True, capture_output=True)

                        if process.returncode:
                            if 'matched no objects' in process.stderr:
                                return 0
                            else:
                                print(f'Error in  {file_path}')
                                errors.append(f'Error in  {file_path}')
                                print(process.stderr)

                        else:
                            # Parse each line in the results
                            return_list = process.stdout.split('\n')[1:-1]
                            if len(return_list) > 0:
                                # Create dict for new directories
                                date_dir_inventory[date] = []
                                # Date directory is new
                                new_dir_count += 1
                                # Iterate through the directories
                                for line in return_list:
                                    try:
                                        id_str = line.split()[-1]
                                        if '.md5' not in id_str:
                                            package_path = f'{date_dir}/{id_str}'
                                            r2r_id = id_str.split('_')[-2]
                                            # Create dict
                                            date_dir_inventory[date].append({'r2r_id': r2r_id,
                                                                             'package_path': package_path})

                                    except Exception as e:
                                        print(f'String Error in {line}')
                                        print(e)
                                        errors.append(e)
                                        continue
                else:
                    # Date is already in the DB
                    continue

        # Errors in Sqlite connection
        except sqlite3.Error as e:
            print(f'Error in SQLITE Connection:  {e}')
            errors.append(e)

        db.close()

        # Check for number of dirs added
        if new_dir_count == 0:
            print('No new directories added to database.')
        else:
            print(f'There were {new_dir_count} new directories'
                  f' added to database.')

        # Finalize dict
        if date_dir_inventory:
            print(date_dir_inventory)
        return date_dir_inventory

    # Errors in subprocess
    except subprocess.CalledProcessError as e:
        print('Error in check_date_dir subprocess:', e)
        print("Standard Error: ", e.stderr)
        errors.append(e)


def build_sqlite(inventory_dict, db_file):

    """
    This function makes a call to the R2R API using
    the fileset ID and then populating the SQLite file
    with metadata

    Args:
        inventory_dict (dict): dict with date dirs as keys
            values are list of
                - r2r_id (int): 5 digit r2r fileset_id
                - package_path (str): sftp path to file for copying
        db_file (str): path to local sqlite file

    Returns:
        Local sqlite file updates

    """

    # Open database connection and create cursor
    [db, curs] = database_connect(db_file)

    # Iterate through the inventory
    # First through each data directory, then through each dataset
    for date, values_list in inventory_dict.items():
        for dataset in values_list:
            # Update variables
            r2r_package_id = dataset['r2r_id']
            server_path = dataset['package_path']

            # Control variables
            fileset_id = None
            update_date = datetime.today()
            file_count = 0
            size_bytes = 0
            human_readable = ''
            instrument = ''
            instrument_type = ''
            cruise_id = ''
            ship = ''
            data_type = ''
            been_pulled = False

            # API Call (Fileset Level)
            try:
                fileset_id = r2r_package_id
                r2r_meta_url = f'https://service.rvdata.us/api/fileset/?fileset_id={fileset_id}'
                r2r_fileset_metadata = requests.get(r2r_meta_url).json()
                fileset_metadata = r2r_fileset_metadata['data'][0]

                # API variables
                # update_date = datetime.today()
                file_count = fileset_metadata['files']
                size_bytes = fileset_metadata['total_bytes']

                human_readable = convert_size(int(size_bytes))
                instrument = fileset_metadata['make_model_name']
                instrument_type = fileset_metadata['device_name']  # This may be helpful for data types
                cruise_id = fileset_metadata['cruise_id']
                ship = fileset_metadata['vessel_name']
                been_pulled = 'N'

                # Different conditions to identify data types
                wcsd_str = ['[water column]', '[Watercolumn]', '[Water Column]', '[watercolumn]']
                if instrument_type == 'Multibeam Sonar':
                    data_type = 'Multibeam'
                    for string in wcsd_str:
                        if string in instrument:
                            data_type = 'WCSD'
                            break
                elif instrument_type == 'Splitbeam Sonar':
                    data_type = 'WCSD'
                else:
                    data_type = 'Trackline'

            # API error
            except requests.RequestException as req_err:
                print(f"An error occurred: {req_err}")
                errors.append(req_err)
                continue

            except Exception as err:
                print(f"An unexpected error occurred: {err}")
                errors.append(err)
                continue

            # Populating the Sqlite file
            try:
                command = ('insert into DATASETS (FILESET_ID, CRUISE_ID, PLATFORM_NAME, INSTRUMENT_NAME,'
                           'INSTRUMENT_TYPE, SIZE_BYTES, HUMAN_READABLE, FILE_COUNT, PACKAGE_PATH,'
                           ' DATE_DIR, DATA_TYPE, BEEN_PULLED) '
                           'values (:fileset_id, :cruise_ID, :platform_name, :instrument_name,'
                           ' :instrument_type, :size_bytes, :human_readable, :file_count, '
                           ':package_path, :date_dir, :data_type, :been_pulled)'
                           )
                data = {'fileset_id': fileset_id,
                        'cruise_ID': cruise_id,
                        'platform_name': ship,
                        'instrument_name': instrument,
                        'instrument_type': instrument_type,
                        'size_bytes': size_bytes,
                        'human_readable': human_readable,
                        'file_count': file_count,
                        'package_path': server_path,
                        'date_dir': date,
                        'data_type': data_type,
                        'been_pulled': been_pulled
                        }
                curs.execute(command, data)
                db.commit()
                print(f'Updated DB with the dataset {cruise_id} {instrument}')

            # Sqlite error
            except sqlite3.Error as e:
                print(f'An error occurred: {e}')
                errors.append(e)
                db.rollback()

    # Close the connection to sqlite file and cursor
    curs.close()
    db.close()
    print('DB update Completed')

    return


def get_data_type(free_space=False):
    """
    Get data type from user prompt

    Args:
        free_space(bool): A True means that there is room on landing space
    Returns:
        name of the data type selected

    """
    # If enough free space on landing
    if free_space:
        prompt = """Enter the number for the desired datatype:
            [1] wcsd - default
            [2] multibeam
            [3] trackline
          >> """
        # User choices
        try:
            choice = input(prompt)
            if choice:
                choice = int(choice)
            if choice == 1 or not choice:
                return "WCSD"
            elif choice == 2:
                return "Multibeam"
            elif choice == 3:
                return "Trackline"
            else:
                print(f"Input {choice} invalid. Try again. ")
        except ValueError as e:
            print(f"Input invalid. ValueError. Try again.")
            errors.append(e)


def query_sqlite(db_file, data_type, free_space):
    """
    Function to interact with user and choose specific
    packages to download based on datatype and free space

    Args:
        db_file: location of sqlite file
        data_type(str): specification of which team's data will be pulled down.
                        Comes from user input
        free_space: This would be the num for bytes of total space - buffer
                    Still need to fix this
    Returns:
        download_list(list): list of file paths that user specified to download
    """

    # Connect to SQLite file and query
    [db, curs] = database_connect(db_file)
    download_list = []

    query = ('SELECT * FROM DATASETS '
             'WHERE BEEN_PULLED = "N" '
             'AND DATA_TYPE LIKE :data')

    data = {'data': data_type}

    curs.execute(query, data)
    query_details = curs.fetchall()
    results = [dict(row) for row in query_details]

    # BYTE Counter + comparison
    byte_counter = 0
    package_dict = {}
    for entry in results:
        size_bytes = entry['SIZE_BYTES']
        query_cruise_id = entry['CRUISE_ID']
        query_instrument = entry['INSTRUMENT_NAME']
        path_to_files = entry['PACKAGE_PATH']
        hr = entry['HUMAN_READABLE']

        if byte_counter < free_space:
            byte_counter += size_bytes
            package_id = f'{query_cruise_id} {query_instrument}'
            package_dict[package_id] = [hr, path_to_files, size_bytes]

    # If packages fit in free space
    # Enumerate used for user input
    if len(package_dict) > 0:
        print('Available packages:')
        for i, (pkg, [size, path, byte]) in enumerate(package_dict.items(), start=1):
            print(f'{i}. {pkg} - {size}')
        choices = (input('Choose what packages you want to download:'
                         '\n\tEnter the numbers separated by commas  EX. 1,5,7 '
                         '\n\tor ranges of the packages using a dash EX. 1-5'
                         '\n>>'
                         ))
        # Support for different input entries
        choice_numbers = set()

        for choice in choices.split(','):
            choice_stripped = choice.strip()
            if '-' in choice:
                # Handle ranges
                start, end = choice_stripped.split('-')
                start = int(start.strip())
                end = int(end.strip())
                choice_numbers.update(range(start, end + 1))
            else:
                # Handle individual numbers
                choice_numbers.add(int(choice_stripped))

        # Check if all choices are valid
        valid_choices = [choice for choice in choice_numbers if 1 <= choice <= len(package_dict)]
        if len(valid_choices) != len(choice_numbers):
            print("Invalid choices detected. Please try again.")
            return None

        # Retrieve the selected packages based on the valid choices
        selected_packages = [list(package_dict.keys())[choice - 1] for choice in sorted(valid_choices)]

        # Display selected packages
        print("\nYou selected:")
        total_data_size = 0
        for pkg in selected_packages:
            print(f"\t{pkg} - {package_dict[pkg][0]}")
            download_list.append(package_dict[pkg][1])
            total_data_size += package_dict[pkg][2]

        total = convert_size(int(total_data_size))
        print(f'\nTotal Requested Data Size: {total}')

    # Package dictionary was not populated
    else:
        print('Query did not return any results')

    # Close DB connection
    db.close()

    # If user made download choices, return list for copying
    if len(download_list) > 0:
        return download_list


def copy_packages(dir_list, landing_path):
    """
    This function takes the list of directories the user chose
    and makes a subprocess call to copy the paths down to the
    proper landing space. It then makes another call to see
    if the name of the file is on the landing zone, it prints
    success and updates the SQLite file to reflect pulldown

    Args:
        dir_list(list): List of paths to download from the server
        landing_path(str): location for files to be copied down to
    Returns:
        print statements to confirm pull down
    """

    # Add the corresponding MD5 files to copy list
    manifests = []
    for file in dir_list:
        name, ext = os.path.splitext(file)
        manifest_file = name + '.md5'
        manifests.append(manifest_file)
    all_paths = dir_list + manifests

    # Open DB connection
    [db, curs] = database_connect(sqlite_file)
    # Copy each path down
    for path in all_paths:
        cd_path, package = os.path.split(path)

        try:
            # If Mission system, use r2r specific command
            if platform.system() != 'Darwin' and platform.system() != 'Windows':
                input_commands = f"""
                cd {cd_path}
                get {package} {landing_path}
                bye
                """
                print(f'\n\nCopying {package} now...')
                process = subprocess.run(['sftp', '-P', port_num, r2r_server_path],
                                         input=input_commands, text=True, capture_output=True)

            # If test system, use fake sftp server
            else:
                input_commands = f"""
                cd {cd_path}
                get {package} {landing_path}
                bye
                """
                process = subprocess.run(['sftp', '-i', 'ssh/ssh_host_rsa_key', '-P', '2222',
                                         'testuser@127.0.0.1'], input=input_commands,
                                         text=True, capture_output=True)

            # If there is an Error --
            if process.stderr:  # gets rid of trailing newline char
                error = process.stderr.split('\n')[:-1]
                if len(error) > 1:
                    print(f'Errors: {error[1:]}')
                    errors.append(error)
                    continue

            # Final Checks
            if process.returncode == 0:
                landing_check = subprocess.run(['ls', landing_path],
                                               text=True, capture_output=True)

                # Check if package is on landing dir
                results = landing_check.stdout[:-1].split()
                if package in results:
                    print(f'\tPackage successfully copied to {landing_path}')

                    # Connect to sqlite and update DB to say it's been pulled
                    command = ('update DATASETS set BEEN_PULLED = :yes '
                               'where PACKAGE_PATH = :path ')
                    values = {'yes': 'Y',
                              'path': path}

                    curs.execute(command, values)
                    db.commit()
                    print(f'{package} has been downloaded onto {landing_path}')

        # Error trap
        except Exception as e:
            print(e)
            errors.append(e)

    # Close database
    db.close()
    return


# Test Calls
# Populating sqlite db
# all_dates = connect_to_sftp(operating_system, port_num, r2r_server_path, ngdc_files_path)
# # all_dates = connect_to_lftp(test_server_name)
# new_inventory = check_date_dirs(all_dates[0], ngdc_files_path, sqlite_file)
# # build_sqlite(new_inventory, sqlite_file)
# copy_packages(test_dir_list, 'path')
# print()


# Prompts and space checks
# space_check = landing_space()
# data_type_input = get_data_type(space_check[1])
# query_sqlite(sqlite_file, 'WCSD', space_check[0])
# query_sqlite(sqlite_file, 'WCSD', 100000000000000)

# Test creds

'''R2R Main Code'''

# Populating sqlite db
all_dates = connect_to_sftp(operating_system, port_num, r2r_server_path, ngdc_files_path)
new_inventory = check_date_dirs(all_dates, ngdc_files_path, sqlite_file)
build_sqlite(new_inventory, sqlite_file)

# Prompts and space checks
space_check = landing_space_bytes(ncei_landing_space='<NCEI Landing Space>')
# Prompts for data types
data_type_input = get_data_type(space_check[1])

# Prompts for which packages to select
pulldown_list = query_sqlite(sqlite_file, data_type_input, space_check[0])
copy_packages(pulldown_list, '<NCEI Landing Space>')

# Print Errors
if errors:
    print(f'\nErrors: \n{errors}')