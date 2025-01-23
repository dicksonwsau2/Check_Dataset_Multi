#!/usr/bin/env python3

import os
import sys
import itertools
import re
import logging
import argparse
from datetime import datetime
from statistics import mean
import csv
import pandas_market_calendars as mcal
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, SpinnerColumn
from multiprocessing import Pool, current_process

def setup_logging(log_filename):
    """
    Set up logging to a file with a timestamped name and configure console logging.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # File handler for detailed logs
    file_handler = logging.FileHandler(log_filename, mode='w')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler for essential messages (only warnings and errors)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)  # Only WARNING and above to console
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

def extract_date_from_filename(filename):
    r"""
    Extract the date part from the filename.

    Returns a tuple of:
      ( normalized_date_str, date_format_label, raw_substring )

    Where:
      - normalized_date_str is 'YYYYMMDD'
      - date_format_label is either 'YYYYMMDD' or 'MM-DD-YYYY'
      - raw_substring is exactly what appeared in the filename
    """
    match_ymd = re.search(r'_(\d{8})\.csv$', filename)
    if match_ymd:
        raw_substring = match_ymd.group(1)  # e.g. "20250111"
        return raw_substring, 'YYYYMMDD', raw_substring

    match_mdy = re.search(r'_(\d{1,2}-\d{1,2}-\d{4})\.csv$', filename)
    if match_mdy:
        raw_substring = match_mdy.group(1)  # e.g. "1-11-2025" or "01-11-2025"
        try:
            date_obj = datetime.strptime(raw_substring, '%m-%d-%Y')
            normalized = date_obj.strftime('%Y%m%d')  # "20250111"
            return normalized, 'MM-DD-YYYY', raw_substring
        except ValueError:
            return None, None, None
    return None, None, None

def parse_arguments():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Comprehensive Dataset-Trades CSV Files Checker.',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        'target_directory',
        type=str,
        help='Path to the target directory containing the dataset files.\n'
             'Example: "D:/Git/trading-blobs/dataset_20240101"'
    )
    parser.add_argument(
        '--multi',
        type=float,
        default=10.0,
        help='Multiplier for abnormal file size detection.\n'
             'Files larger than (mean * multiplier) or smaller than (mean / multiplier) are flagged.\n'
             'Default: 10.0'
    )
    parser.add_argument(
        '--s-date',
        type=str,
        required=True,
        help='Start date for EntryTime in YYYYMMDD format.\n'
             'Example: "20230101"'
    )
    parser.add_argument(
        '--e-date',
        type=str,
        required=True,
        help='End date for EntryTime in YYYYMMDD format.\n'
             'Example: "20241122"'
    )
    # NEW ARGUMENT: --endcheck in DD/MM/YYYY
    parser.add_argument(
        '--endcheck',
        type=str,
        required=False,
        help='Check that the last date in each CSV matches this DD/MM/YYYY date.\n'
             'Example: "--endcheck 17/01/2025"'
    )
    return parser.parse_args()

def validate_date_format(date_str):
    """
    Validate that the date string is in YYYYMMDD format.
    """
    try:
        datetime.strptime(date_str, '%Y%m%d')
        return True
    except ValueError:
        return False

def load_spx_trading_dates(s_date, e_date):
    """
    Retrieve SPX trading dates between the start and end dates using pandas_market_calendars.
    """
    import pandas_market_calendars as mcal
    try:
        nyse = mcal.get_calendar('NYSE')
        start_dt = datetime.strptime(s_date, '%Y%m%d')
        end_dt = datetime.strptime(e_date, '%Y%m%d')
        schedule = nyse.schedule(start_date=start_dt, end_date=end_dt)
        trading_days = mcal.date_range(schedule, frequency='1D').strftime('%Y-%m-%d').tolist()
        return set(trading_days)
    except Exception as e:
        logging.error(f"Error retrieving SPX trading dates: {e}")
        sys.exit(1)

def log_parameter_lists(prem_list, width_list, sl_list, ema_list):
    """
    Log the parameter lists to the log file.
    """
    logging.info("Parameter Lists:")
    logging.info(f"prem: {', '.join(prem_list)}")
    logging.info(f"width: {', '.join(width_list)}")
    logging.info(f"sl: {', '.join(sl_list)}")
    logging.info(f"ema: {', '.join(ema_list)}\n")

def parse_width_range(width_str):
    """
    Parse the width string to determine if it's a range or a fixed value.
    """
    if '-' in width_str:
        parts = width_str.split('-')
        try:
            values = set(float(part) for part in parts)
            return ('range', values)
        except ValueError:
            return ('invalid',)
    else:
        try:
            value = float(width_str)
            return ('fixed', value)
        except ValueError:
            return ('invalid',)

def parse_entry_time(entry_time_str):
    """
    Parse the EntryTime string using multiple possible formats.
    """
    formats = [
        '%Y-%m-%d %H:%M:%S',   # e.g. 2023-01-01 10:30:00
        '%m/%d/%Y %I:%M:%S %p' # e.g. 01/17/2025 10:45:00 AM
    ]
    for fmt in formats:
        try:
            return datetime.strptime(entry_time_str, fmt)
        except ValueError:
            continue
    return None

# Global variables for multiprocessing
global_target_dir = None
global_filename_pattern = None
global_trading_dates = None
global_endcheck_date = None  # <--- We'll store the parsed date here (if provided)

def init_pool(target_dir_param, filename_pattern_param, trading_dates_param, endcheck_date_param):
    global global_target_dir, global_filename_pattern, global_trading_dates, global_endcheck_date
    global_target_dir = target_dir_param
    global_filename_pattern = filename_pattern_param
    global_trading_dates = trading_dates_param
    global_endcheck_date = endcheck_date_param

def process_csv_file(file):
    """
    Read and validate each CSV file. 
    Also capture the last date if --endcheck is used.
    """
    global global_target_dir, global_filename_pattern, global_trading_dates, global_endcheck_date
    results = {
        'file': file,
        'width_sl_mismatches': [],
        'duplicate_entry_times': set(),
        'entrytime_date_mismatches': [],
        'entrytime_dates_set': set(),
        'messages': [],
        'last_csv_date': None,  # Will store the last date/time found if we can parse
    }
    try:
        filepath = os.path.join(global_target_dir, file)
        match = global_filename_pattern.match(file)
        if not match:
            message = f"Filename '{file}' does not match the expected pattern."
            results['messages'].append(message)
            return results

        prem_f, width_f, sl_f, ema_f, date_f = match.groups()
        # Extract date from filename
        norm_date, date_format, raw_date_str = extract_date_from_filename(file)
        if not norm_date:
            message = f"No valid date found in filename '{file}'. Skipping content validation."
            results['messages'].append(message)
            return results

        # Read CSV content
        with open(filepath, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            necessary_columns = {'Width', 'StopLossTarget', 'EntryTime'}
            if not necessary_columns.issubset(set(reader.fieldnames)):
                missing_cols = necessary_columns - set(reader.fieldnames)
                message = f"File '{file}' is missing columns: {', '.join(missing_cols)}. Skipping file."
                results['messages'].append(message)
                return results

            widths_in_csv = set()
            sls_in_csv = set()
            entry_times = set()
            duplicate_entry_times = set()
            entry_dates = set()

            # We'll capture the last row's date in "EntryTime"
            last_date_in_csv = None

            for row in reader:
                # Collect Width, StopLossTarget
                width_csv = row.get('Width', '').strip()
                sl_csv = row.get('StopLossTarget', '').strip()
                if width_csv:
                    try:
                        widths_in_csv.add(float(width_csv))
                    except ValueError:
                        msg = f"{file}, Width, Non-numeric value, Found: {width_csv}"
                        results['messages'].append(msg)
                if sl_csv:
                    sls_in_csv.add(sl_csv)

                # Collect EntryTime
                entry_time_str = row.get('EntryTime', '').strip()
                if entry_time_str:
                    # Check for duplicate
                    if entry_time_str in entry_times:
                        duplicate_entry_times.add(entry_time_str)
                    else:
                        entry_times.add(entry_time_str)

                    # Parse date/time from EntryTime to check trading date
                    dt = parse_entry_time(entry_time_str)
                    if dt:
                        entry_date = dt.date()
                        entry_dates.add(entry_date)
                        # Keep track of the last row we see
                        last_date_in_csv = dt
                    else:
                        msg = f"{file}, EntryTime, Unsupported format, Found: {entry_time_str}"
                        results['messages'].append(msg)

            # Store the last date found in the CSV (if any) in results
            results['last_csv_date'] = last_date_in_csv

            # Now do the expected width checks
            width_parse = parse_width_range(width_f)
            if width_parse[0] == 'range':
                width_expected_set = width_parse[1]
                width_expected_type = 'range'
            elif width_parse[0] == 'fixed':
                width_expected = width_parse[1]
                width_expected_type = 'fixed'
            else:
                msg = f"{file}, Width, Invalid format, Found: {width_f}"
                results['messages'].append(msg)
                results['width_sl_mismatches'].append((file, 'Width', 'Invalid format', widths_in_csv))
                width_expected_type = 'invalid'

            if width_expected_type == 'range':
                invalid_widths = [w for w in widths_in_csv if w not in width_expected_set]
                if invalid_widths:
                    results['width_sl_mismatches'].append(
                        (file, 'Width', f"Expected: {width_expected_set}", invalid_widths)
                    )
            elif width_expected_type == 'fixed':
                if width_expected not in widths_in_csv:
                    results['width_sl_mismatches'].append(
                        (file, 'Width', f"Expected: {width_expected}", widths_in_csv)
                    )

            # StopLossTarget check
            expected_sl = sl_f  # e.g., '1x', '1.25x', etc.
            if expected_sl:
                if not sls_in_csv:
                    msg = f"{file}, StopLossTarget, Expected: {expected_sl}, Found: set()"
                    results['messages'].append(msg)
                    results['width_sl_mismatches'].append((file, 'StopLossTarget', expected_sl, set()))
                else:
                    if expected_sl not in sls_in_csv:
                        results['width_sl_mismatches'].append((file, 'StopLossTarget', expected_sl, sls_in_csv))
            else:
                msg = f"{file}, StopLossTarget, No expected value provided."
                results['messages'].append(msg)

            # Duplicates
            if duplicate_entry_times:
                results['duplicate_entry_times'] = duplicate_entry_times

            # Check entry dates vs. trading calendar
            for entry_date in entry_dates:
                entry_date_str = entry_date.strftime('%Y-%m-%d')
                if entry_date_str not in global_trading_dates:
                    results['entrytime_date_mismatches'].append((file, entry_date_str))
                results['entrytime_dates_set'].add(entry_date_str)

    except Exception as e:
        message = f"Error processing file '{file}': {e}. Skipping file."
        results['messages'].append(message)
        return results

    return results

def main():
    args = parse_arguments()
    target_dir = args.target_directory
    multi = args.multi
    s_date = args.s_date
    e_date = args.e_date
    endcheck_str = args.endcheck  # The raw string for --endcheck (DD/MM/YYYY)

    # Validate s_date, e_date
    if s_date and e_date:
        if validate_date_format(s_date) and validate_date_format(e_date):
            if s_date > e_date:
                print("Error: --s-date cannot be later than --e-date.")
                sys.exit(1)
        else:
            print("Error: --s-date and --e-date must be in YYYYMMDD format.")
            sys.exit(1)
    else:
        print("Error: Both --s-date and --e-date must be provided.")
        sys.exit(1)

    # Attempt to parse endcheck if provided
    endcheck_date = None
    if endcheck_str:
        try:
            # We'll parse just the date (DD/MM/YYYY) ignoring any time
            endcheck_date = datetime.strptime(endcheck_str, '%d/%m/%Y').date()
        except ValueError:
            print(f"Error: --endcheck '{endcheck_str}' is not in DD/MM/YYYY format.")
            sys.exit(1)

    # Set up logging
    log_filename = datetime.now().strftime('log_%Y%m%d_%H%M%S.log')
    setup_logging(log_filename)

    print(f"Logging initialized. Log file: {log_filename}")
    print(f"Target directory: {target_dir}")
    print(f"Size multiplier for abnormal files: {multi}")
    print(f"Date range for EntryTime: {s_date} to {e_date} (inclusive)")
    if endcheck_date:
        print(f"Will check last CSV date matches: {endcheck_date.strftime('%d/%m/%Y')}\n")

    # Load trading dates
    trading_dates = load_spx_trading_dates(s_date, e_date)
    print(f"Number of SPX trading dates retrieved: {len(trading_dates)}\n")

    # Define parameter lists
    prem_list = [
        "1.0", "1.2", "1.4", "1.6", "1.8", 
        "2.0", "2.2", "2.4", "2.6", "2.8",
        "3.0", "3.2", "3.4", "3.6", "3.8", 
        "4.0", "4.2", "4.4", "4.6", "4.8",          
        "5.0"
    ]
    width_list = [
        "5",
        "10", "15", "20", "25", "30",
        "35", "40", "45", "50", "55",
        "20-25-30", "45-50-55"
    ]
    sl_list = ["1x", "1.25x", "1.5x", "2x"]
    ema_list = ["540", "520", "2040"]

    # Log parameter lists
    log_parameter_lists(prem_list, width_list, sl_list, ema_list)

    total_expected = len(prem_list) * len(width_list) * len(sl_list) * len(ema_list)
    print(f"Total expected filenames: {total_expected}")
    logging.info(f"Total expected filenames: {total_expected}")

    # Check directory
    if not os.path.isdir(target_dir):
        logging.error(f"Error: The directory '{target_dir}' does not exist or is not accessible.")
        print(f"Error: The directory '{target_dir}' does not exist or is not accessible.")
        sys.exit(1)

    try:
        all_files = os.listdir(target_dir)
    except Exception as e:
        logging.error(f"Error accessing the directory '{target_dir}': {e}")
        print(f"Error accessing the directory '{target_dir}': {e}")
        sys.exit(1)

    csv_files = [f for f in all_files if f.startswith('Dataset-Trades_') and f.endswith('.csv')]
    print(f"Total CSV files found: {len(csv_files)}\n")
    logging.info(f"Total CSV files found: {len(csv_files)}\n")

    # Extract date info
    dates_formats = {}
    for file in csv_files:
        norm_date, fmt_label, raw_str = extract_date_from_filename(file)
        if norm_date:
            if norm_date not in dates_formats:
                dates_formats[norm_date] = set()
            dates_formats[norm_date].add((fmt_label, raw_str))

    if not dates_formats:
        logging.error("No valid date found in any filenames.")
        print("No valid date found in any filenames.")
        sys.exit(1)

    latest_date = max(dates_formats.keys())  # e.g. '20250111'
    print(f"Latest date found in filenames: {latest_date}\n")
    logging.info(f"Latest date found in filenames: {latest_date}\n")

    latest_date_info = dates_formats[latest_date]
    print("Date formats found for latest date (and raw date substrings):")
    for (lbl, raw) in latest_date_info:
        print(f"  {lbl} => {raw}")
    print()
    logging.info(f"Date formats found for latest date: {latest_date_info}\n")

    # Generate expected filenames for the "latest_date" substrings
    expected_filenames = set()
    for (fmt_label, raw_str) in latest_date_info:
        for prem, width, sl, ema in itertools.product(prem_list, width_list, sl_list, ema_list):
            filename = f"Dataset-Trades_{prem}_{width}_{sl}_EMA{ema}_{raw_str}.csv"
            expected_filenames.add(filename)

    print(f"Checking for files with normalized date: {latest_date}\n")
    logging.info(f"Checking for files with normalized date: {latest_date}\n")

    existing_filenames = set(csv_files).intersection(expected_filenames)
    found_files = len(existing_filenames)
    missing_files = expected_filenames - existing_filenames
    missing_count = len(missing_files)
    missing_percentage = (missing_count / total_expected) * 100

    print(f"Files found: {found_files}")
    print(f"Files missing: {missing_count}")
    print(f"Missing files percentage: {missing_percentage:.2f}%\n")
    logging.info(f"Files found: {found_files}")
    logging.info(f"Files missing: {missing_count}")
    logging.info(f"Missing files percentage: {missing_percentage:.2f}%\n")

    if missing_files:
        print("List of missing filenames:")
        for fname in sorted(missing_files):
            print(fname)
        logging.info("List of missing filenames:")
        for fname in sorted(missing_files):
            logging.info(fname)
    else:
        print("All expected files are present.")
        logging.info("All expected files are present.")

    # -------------------- File Size Analysis --------------------
    print("\n--- File Size Analysis ---")
    logging.info("\n--- File Size Analysis ---")
    file_sizes = []
    for file in csv_files:
        filepath = os.path.join(target_dir, file)
        try:
            size = os.path.getsize(filepath)
            file_sizes.append((file, size))
        except Exception as e:
            logging.warning(f"Could not get size for file '{file}': {e}")
            print(f"Warning: Could not get size for file '{file}': {e}")

    if not file_sizes:
        logging.warning("No file sizes could be determined.")
        print("Warning: No file sizes could be determined.")
    else:
        sizes = [sz for _, sz in file_sizes]
        mean_size = mean(sizes)
        threshold_upper = mean_size * multi
        threshold_lower = mean_size / multi

        print(f"Mean file size: {mean_size:.2f} bytes")
        print(f"Upper threshold (mean * {multi}): {threshold_upper:.2f} bytes")
        print(f"Lower threshold (mean / {multi}): {threshold_lower:.2f} bytes\n")
        logging.info(f"Mean file size: {mean_size:.2f} bytes")
        logging.info(f"Upper threshold (mean * {multi}): {threshold_upper:.2f} bytes")
        logging.info(f"Lower threshold (mean / {multi}): {threshold_lower:.2f} bytes")

        abnormal_files = []
        for file, size in file_sizes:
            if size > threshold_upper or size < threshold_lower:
                abnormal_files.append((file, size))

        print(f"Number of abnormal files: {len(abnormal_files)}")
        logging.info(f"Number of abnormal files: {len(abnormal_files)}")
        if abnormal_files:
            print("List of abnormal files (filename, size in bytes):")
            logging.info("List of abnormal files (filename, size in bytes):")
            for file, size in sorted(abnormal_files, key=lambda x: x[1], reverse=True):
                print(f"{file}, {size} bytes")
                logging.info(f"{file}, {size} bytes")
        else:
            print("No abnormal files detected based on the specified multiplier.")
            logging.info("No abnormal files detected based on the specified multiplier.")

    # -------------------- CSV Content Validation --------------------
    print("\n--- CSV Content Validation ---")
    logging.info("\n--- CSV Content Validation ---")

    filename_pattern = re.compile(r'^Dataset-Trades_([\d.]+)_([\d\-x]+)_([\dx\.]+)_EMA(\d+)_([\d\-]+)\.csv$')

    global global_target_dir, global_filename_pattern, global_trading_dates, global_endcheck_date
    global_target_dir = target_dir
    global_filename_pattern = filename_pattern
    global_trading_dates = trading_dates
    global_endcheck_date = endcheck_date  # We'll pass this to the worker

    total_csv_checked = 0
    width_sl_mismatches = []
    duplicate_entrytime_files = []
    entrytime_date_mismatches = []
    entrytime_dates_set = set()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Validating CSV files...", total=len(csv_files))

        # Use multiprocessing Pool
        pool = Pool(
            initializer=init_pool,
            initargs=(target_dir, filename_pattern, trading_dates, endcheck_date)
        )
        results_list = []
        for result in pool.imap_unordered(process_csv_file, csv_files):
            progress.advance(task)
            results_list.append(result)
        pool.close()
        pool.join()

    # Aggregate results
    for result in results_list:
        total_csv_checked += 1
        width_sl_mismatches.extend(result['width_sl_mismatches'])
        if result['duplicate_entry_times']:
            duplicate_entrytime_files.append(
                (result['file'], result['duplicate_entry_times'])
            )
        entrytime_date_mismatches.extend(result['entrytime_date_mismatches'])
        entrytime_dates_set.update(result['entrytime_dates_set'])

        # Log messages
        for message in result['messages']:
            logging.info(message)

    logging.info(f"\nTotal CSV files checked: {total_csv_checked}")
    print(f"\nTotal CSV files checked: {total_csv_checked}")
    logging.info(f"Number of files with Width or StopLossTarget mismatches: {len(width_sl_mismatches)}")
    print(f"Number of files with Width or StopLossTarget mismatches: {len(width_sl_mismatches)}")
    if width_sl_mismatches:
        print("List of files with mismatches (filename, field, expected, found values):")
        logging.info("List of files with mismatches (filename, field, expected, found values):")
        for file, field, expected, found in width_sl_mismatches:
            print(f"{file}, {field}, Expected: {expected}, Found: {found}")
            logging.info(f"{file}, {field}, Expected: {expected}, Found: {found}")
    else:
        print("No Width or StopLossTarget mismatches found.")
        logging.info("No Width or StopLossTarget mismatches found.")

    # Duplicates
    logging.info(f"\nNumber of files with duplicate EntryTime entries: {len(duplicate_entrytime_files)}")
    print(f"\nNumber of files with duplicate EntryTime entries: {len(duplicate_entrytime_files)}")
    if duplicate_entrytime_files:
        print("List of files with duplicate EntryTime entries:")
        logging.info("List of files with duplicate EntryTime entries:")
        for file, duplicates in duplicate_entrytime_files:
            duplicates_str = ', '.join(duplicates)
            print(f"{file}, Duplicates: {duplicates_str}")
            logging.info(f"{file}, Duplicates: {duplicates_str}")
    else:
        print("No duplicate EntryTime entries found.")
        logging.info("No duplicate EntryTime entries found.")

    # EntryTime date mismatches
    logging.info(f"\nNumber of files with EntryTime date mismatches: {len(entrytime_date_mismatches)}")
    print(f"\nNumber of files with EntryTime date mismatches: {len(entrytime_date_mismatches)}")
    if entrytime_date_mismatches:
        print("List of files with EntryTime dates not matching SPX trading dates:")
        logging.info("List of files with EntryTime dates not matching SPX trading dates:")
        for file, date_str in entrytime_date_mismatches:
            print(f"{file}, EntryDate: {date_str}")
            logging.info(f"{file}, EntryDate: {date_str}")
    else:
        print("All EntryTime dates match SPX trading dates.")
        logging.info("All EntryTime dates match SPX trading dates.")

    # Missing SPX dates
    print("\n--- Missing SPX Trading Dates ---")
    logging.info("\n--- Missing SPX Trading Dates ---")
    missing_trading_dates = trading_dates - entrytime_dates_set
    if missing_trading_dates:
        print(f"Number of SPX trading dates missing from EntryTime data: {len(missing_trading_dates)}")
        print("List of missing SPX trading dates:")
        logging.info(f"Number of SPX trading dates missing from EntryTime data: {len(missing_trading_dates)}")
        logging.info("List of missing SPX trading dates:")
        for date_str in sorted(missing_trading_dates):
            print(date_str)
            logging.info(date_str)
    else:
        print("No SPX trading dates are missing from EntryTime data.")
        logging.info("No SPX trading dates are missing from EntryTime data.")

    # -------------------- Endcheck Mismatch Logging --------------------
    if endcheck_date:
        mismatch_log_filename = "endcheck_mismatch.log"
        mismatched_files = []
        for res in results_list:
            last_dt = res['last_csv_date']
            if last_dt is None:
                # If no valid date was found at all, treat as mismatch
                mismatched_files.append(res['file'])
            else:
                # Compare only the date portion
                if last_dt.date() != endcheck_date:
                    mismatched_files.append(res['file'])

        # Write mismatched filenames to endcheck_mismatch.log
        if mismatched_files:
            with open(mismatch_log_filename, 'w') as f:
                for filename in mismatched_files:
                    f.write(filename + "\n")

            print(f"\n--- Endcheck Mismatches ---")
            # Only print the count to console:
            print(f"Number of CSV files that do NOT have a last date of "
                f"{endcheck_date.strftime('%d/%m/%Y')}: {len(mismatched_files)}")
            print(f"(See '{mismatch_log_filename}' for the full mismatch list.)")

        else:
            print(f"\nAll CSV files match the last date {endcheck_date.strftime('%d/%m/%Y')}.")
        
        # Keep track of mismatch_count for Summary Statistics
        mismatch_count = len(mismatched_files)

    else:
        mismatch_count = 0


    # -------------------- Summary Statistics --------------------
        print("\n--- Summary Statistics ---")
        logging.info("\n--- Summary Statistics ---")

        print(f"Total expected filenames: {total_expected}")
        logging.info(f"Total expected filenames: {total_expected}")

        print(f"Files found: {found_files}")
        logging.info(f"Files found: {found_files}")

        print(f"Files missing: {missing_count} ({missing_percentage:.2f}%)")
        logging.info(f"Files missing: {missing_count} ({missing_percentage:.2f}%)")

        print(f"Total CSV files processed: {total_csv_checked}")
        logging.info(f"Total CSV files processed: {total_csv_checked}")

        if 'abnormal_files' in locals():
            print(f"Total abnormal files based on size: {len(abnormal_files)}")
            logging.info(f"Total abnormal files based on size: {len(abnormal_files)}")
        else:
            print("Total abnormal files based on size: 0")
            logging.info("Total abnormal files based on size: 0")

        print(f"Total Width/StopLossTarget mismatches: {len(width_sl_mismatches)}")
        logging.info(f"Total Width/StopLossTarget mismatches: {len(width_sl_mismatches)}")

        print(f"Total duplicate EntryTime entries: {len(duplicate_entrytime_files)}")
        logging.info(f"Total duplicate EntryTime entries: {len(duplicate_entrytime_files)}")

        print(f"Total EntryTime date mismatches: {len(entrytime_date_mismatches)}")
        logging.info(f"Total EntryTime date mismatches: {len(entrytime_date_mismatches)}")

        print(f"Total missing SPX trading dates: {len(missing_trading_dates)}")
        logging.info(f"Total missing SPX trading dates: {len(missing_trading_dates)}")

        # NEW: Print and log mismatch_count if you used the --endcheck option
        print(f"Total mismatch CSV for endcheck date: {mismatch_count}")
        logging.info(f"Total mismatch CSV for endcheck date: {mismatch_count}")

        print("\n--- Script Execution Completed ---")
        logging.info("\n--- Script Execution Completed ---")




if __name__ == "__main__":
    main()
