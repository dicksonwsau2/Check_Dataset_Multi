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

    Parameters:
        log_filename (str): The name of the log file to create.
    """
    # Create logger
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
      - normalized_date_str is 'YYYYMMDD' (e.g. "20250111")
      - date_format_label is either 'YYYYMMDD' or 'MM-DD-YYYY'
      - raw_substring is exactly what appeared in the filename
        (e.g. "1-11-2025" vs. "01-11-2025")

    Example filenames:
       Dataset-Trades_1.0_5_1.5x_EMA520_1-11-2025.csv
       Dataset-Trades_1.0_5_1.5x_EMA520_01-11-2025.csv
       Dataset-Trades_1.0_5_1.5x_EMA520_20250111.csv
    """
    # Pattern for YYYYMMDD
    match_ymd = re.search(r'_(\d{8})\.csv$', filename)
    if match_ymd:
        raw_substring = match_ymd.group(1)  # e.g. "20250111"
        return raw_substring, 'YYYYMMDD', raw_substring
    
    # Pattern for M-D-YYYY or MM-DD-YYYY
    match_mdy = re.search(r'_(\d{1,2}-\d{1,2}-\d{4})\.csv$', filename)
    if match_mdy:
        raw_substring = match_mdy.group(1)  # e.g. "1-11-2025" or "01-11-2025"
        try:
            # Convert M-D-YYYY (or MM-DD-YYYY) to YYYYMMDD
            date_obj = datetime.strptime(raw_substring, '%m-%d-%Y')
            normalized = date_obj.strftime('%Y%m%d')  # "20250111"
            return normalized, 'MM-DD-YYYY', raw_substring
        except ValueError:
            return None, None, None
    
    return None, None, None

def parse_arguments():
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: The parsed command-line arguments.
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
             'Files larger than (mean * multiplier) or smaller than (mean / multiplier) are flagged as abnormal.\n'
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
    return parser.parse_args()

def validate_date_format(date_str):
    """
    Validate that the date string is in YYYYMMDD format.

    Parameters:
        date_str (str): The date string to validate.

    Returns:
        bool: True if valid, False otherwise.
    """
    try:
        datetime.strptime(date_str, '%Y%m%d')
        return True
    except ValueError:
        return False

def load_spx_trading_dates(s_date, e_date):
    """
    Retrieve SPX trading dates between the start and end dates using pandas_market_calendars.

    Parameters:
        s_date (str): Start date in YYYYMMDD format.
        e_date (str): End date in YYYYMMDD format.

    Returns:
        set: A set of trading dates in 'YYYY-MM-DD' format.
    """
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
    formats = ['%Y-%m-%d %H:%M:%S', '%m/%d/%Y %I:%M:%S %p']
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

def init_pool(target_dir_param, filename_pattern_param, trading_dates_param):
    global global_target_dir, global_filename_pattern, global_trading_dates
    global_target_dir = target_dir_param
    global_filename_pattern = filename_pattern_param
    global_trading_dates = trading_dates_param

def process_csv_file(file):
    global global_target_dir, global_filename_pattern, global_trading_dates
    results = {
        'file': file,
        'width_sl_mismatches': [],
        'duplicate_entry_times': set(),
        'entrytime_date_mismatches': [],
        'entrytime_dates_set': set(),
        'messages': [],
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
            # Verify necessary columns
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

            for row in reader:
                # Collect Width and StopLossTarget from CSV
                width_csv = row.get('Width', '').strip()
                sl_csv = row.get('StopLossTarget', '').strip()
                if width_csv:
                    try:
                        widths_in_csv.add(float(width_csv))
                    except ValueError:
                        message = f"{file}, Width, Non-numeric value, Found: {width_csv}"
                        results['messages'].append(message)
                if sl_csv:
                    sls_in_csv.add(sl_csv)
                
                # Collect EntryTime
                entry_time_str = row.get('EntryTime', '').strip()
                if entry_time_str:
                    # Check for duplicate EntryTime
                    if entry_time_str in entry_times:
                        duplicate_entry_times.add(entry_time_str)
                    else:
                        entry_times.add(entry_time_str)
                    # Extract date from EntryTime
                    entry_datetime = parse_entry_time(entry_time_str)
                    if entry_datetime:
                        entry_date = entry_datetime.date()
                        entry_dates.add(entry_date)
                    else:
                        message = f"{file}, EntryTime, Unsupported format, Found: {entry_time_str}"
                        results['messages'].append(message)

            # Parse and validate Width
            width_parse = parse_width_range(width_f)
            if width_parse[0] == 'range':
                width_expected_set = width_parse[1]
                width_expected_type = 'range'
            elif width_parse[0] == 'fixed':
                width_expected = width_parse[1]
                width_expected_type = 'fixed'
            else:
                message = f"{file}, Width, Invalid format, Found: {width_f}"
                results['messages'].append(message)
                results['width_sl_mismatches'].append((file, 'Width', 'Invalid format', widths_in_csv))
                width_expected_type = 'invalid'

            # Validate Width
            if width_expected_type == 'range':
                # Check if all CSV widths are within the expected set
                invalid_widths = [w for w in widths_in_csv if w not in width_expected_set]
                if invalid_widths:
                    results['width_sl_mismatches'].append((file, 'Width', f"Expected: {width_expected_set}", invalid_widths))
            elif width_expected_type == 'fixed':
                if width_expected not in widths_in_csv:
                    results['width_sl_mismatches'].append((file, 'Width', f"Expected: {width_expected}", widths_in_csv))
            # else: already logged as invalid

            # Validate StopLossTarget
            expected_sl = sl_f  # e.g., '1x', '1.25x', etc.
            if expected_sl:
                if not sls_in_csv:
                    # No StopLossTarget values found
                    message = f"{file}, StopLossTarget, Expected: {expected_sl}, Found: set()"
                    results['messages'].append(message)
                    results['width_sl_mismatches'].append((file, 'StopLossTarget', expected_sl, set()))
                else:
                    if expected_sl not in sls_in_csv:
                        results['width_sl_mismatches'].append((file, 'StopLossTarget', expected_sl, sls_in_csv))
            else:
                message = f"{file}, StopLossTarget, No expected value provided."
                results['messages'].append(message)

            # Check for duplicate EntryTime
            if duplicate_entry_times:
                results['duplicate_entry_times'] = duplicate_entry_times
            
            # Check EntryTime dates against SPX trading dates
            for entry_date in entry_dates:
                entry_date_str = entry_date.strftime('%Y-%m-%d')
                if entry_date_str not in global_trading_dates:
                    results['entrytime_date_mismatches'].append((file, entry_date_str))
                # Collect all EntryTime dates
                results['entrytime_dates_set'].add(entry_date_str)

    except Exception as e:
        message = f"Error processing file '{file}': {e}. Skipping file."
        results['messages'].append(message)
        return results

    return results

def main():
    """
    Main function to execute the CSV validation script.

    Performs the following steps:
    1. Parses command-line arguments.
    2. Sets up logging.
    3. Validates the target directory and date range.
    4. Logs parameter lists.
    5. Checks for missing files.
    6. Analyzes file sizes.
    7. Validates CSV content with progress indication.
    8. Summarizes validation results.
    """
    # Parse arguments
    args = parse_arguments()
    target_dir = args.target_directory
    multi = args.multi
    s_date = args.s_date
    e_date = args.e_date

    # Validate and process date range
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

    # Set up logging
    log_filename = datetime.now().strftime('log_%Y%m%d_%H%M%S.log')
    setup_logging(log_filename)

    # Print high-level messages to console without timestamps or log levels
    print(f"Logging initialized. Log file: {log_filename}")
    print(f"Target directory: {target_dir}")
    print(f"Size multiplier for abnormal files: {multi}")
    print(f"Date range for EntryTime: {s_date} to {e_date} (inclusive)")

    # Load SPX trading dates
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

    # Calculate total expected combos
    total_expected = len(prem_list) * len(width_list) * len(sl_list) * len(ema_list)
    print(f"Total expected filenames: {total_expected}")
    logging.info(f"Total expected filenames: {total_expected}")

    # List all files
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

    # Dictionary: normalized_date -> set of (format_label, raw_substring)
    dates_formats = {}
    for file in csv_files:
        norm_date, fmt_label, raw_str = extract_date_from_filename(file)
        if norm_date:
            if norm_date not in dates_formats:
                dates_formats[norm_date] = set()
            # Store the tuple (fmt_label, raw_str).
            # e.g. ('MM-DD-YYYY', '1-11-2025')
            dates_formats[norm_date].add((fmt_label, raw_str))

    if not dates_formats:
        logging.error("No valid date found in any filenames.")
        print("No valid date found in any filenames.")
        sys.exit(1)

    # Find the latest date in normalized form
    latest_date = max(dates_formats.keys())  # e.g. '20250111'
    print(f"Latest date found in filenames: {latest_date}\n")
    logging.info(f"Latest date found in filenames: {latest_date}\n")

    # All (fmt_label, raw_substring) combos for that date
    latest_date_info = dates_formats[latest_date]
    # Example: { ('MM-DD-YYYY', '1-11-2025'), ('MM-DD-YYYY', '01-11-2025') }

    print("Date formats found for latest date (and raw date substrings):")
    for (lbl, raw) in latest_date_info:
        print(f"  {lbl} => {raw}")
    print()
    logging.info(f"Date formats found for latest date: {latest_date_info}\n")

    # Generate "expected_filenames" by reusing the exact raw substring
    expected_filenames = set()
    for (fmt_label, raw_str) in latest_date_info:
        # For each date-substring seen in the actual files, generate all permutations
        for prem, width, sl, ema in itertools.product(prem_list, width_list, sl_list, ema_list):
            filename = f"Dataset-Trades_{prem}_{width}_{sl}_EMA{ema}_{raw_str}.csv"
            expected_filenames.add(filename)

    print(f"Checking for files with normalized date: {latest_date}\n")
    logging.info(f"Checking for files with normalized date: {latest_date}\n")

    # Now see which of those "expected_filenames" exist
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
        sizes = [size for _, size in file_sizes]
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

    # Define the regular expression pattern for filenames
    filename_pattern = re.compile(r'^Dataset-Trades_([\d.]+)_([\d\-x]+)_([\dx\.]+)_EMA(\d+)_([\d\-]+)\.csv$')

    # Initialize global variables for child processes
    global global_target_dir, global_filename_pattern, global_trading_dates
    global_target_dir = target_dir
    global_filename_pattern = filename_pattern
    global_trading_dates = trading_dates

    # Counters for summary
    total_csv_checked = 0
    width_sl_mismatches = []
    duplicate_entrytime_files = []
    entrytime_date_mismatches = []
    entrytime_dates_set = set()

    # Initialize progress bar with ETA
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
        pool = Pool(initializer=init_pool, initargs=(target_dir, filename_pattern, trading_dates))
        results_list = []
        for result in pool.imap_unordered(process_csv_file, csv_files):
            progress.advance(task)
            results_list.append(result)
        pool.close()
        pool.join()

    # Aggregate results
    for result in results_list:
        total_csv_checked += 1
        # Collect per-file results
        width_sl_mismatches.extend(result['width_sl_mismatches'])
        if result['duplicate_entry_times']:
            duplicate_entrytime_files.append((result['file'], result['duplicate_entry_times']))
        entrytime_date_mismatches.extend(result['entrytime_date_mismatches'])
        entrytime_dates_set.update(result['entrytime_dates_set'])
        # Log messages
        for message in result['messages']:
            logging.info(message)

    # Report Width/StopLossTarget mismatches
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

    # Report duplicate EntryTime
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

    # Report EntryTime date mismatches
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

    # -------------------- Missing SPX Trading Dates --------------------
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

    # -------------------- Summary Statistics --------------------
    print("\n--- Summary Statistics ---")
    logging.info("\n--- Summary Statistics ---")
    print(f"Total expected filenames: {total_expected}")
    print(f"Files found: {found_files}")
    print(f"Files missing: {missing_count} ({missing_percentage:.2f}%)")
    print(f"Total CSV files processed: {total_csv_checked}")
    # If abnormal_files was computed, display it
    if 'abnormal_files' in locals():
        print(f"Total abnormal files based on size: {len(abnormal_files)}")
    else:
        print("Total abnormal files based on size: 0")
    print(f"Total Width/StopLossTarget mismatches: {len(width_sl_mismatches)}")
    print(f"Total duplicate EntryTime entries: {len(duplicate_entrytime_files)}")
    print(f"Total EntryTime date mismatches: {len(entrytime_date_mismatches)}")
    print(f"Total missing SPX trading dates: {len(missing_trading_dates)}")
    logging.info(f"Total expected filenames: {total_expected}")
    logging.info(f"Files found: {found_files}")
    logging.info(f"Files missing: {missing_count} ({missing_percentage:.2f}%)")
    logging.info(f"Total CSV files processed: {total_csv_checked}")
    logging.info(f"Total abnormal files based on size: {len(abnormal_files) if 'abnormal_files' in locals() else 0}")
    logging.info(f"Total Width/StopLossTarget mismatches: {len(width_sl_mismatches)}")
    logging.info(f"Total duplicate EntryTime entries: {len(duplicate_entrytime_files)}")
    logging.info(f"Total EntryTime date mismatches: {len(entrytime_date_mismatches)}")
    logging.info(f"Total missing SPX trading dates: {len(missing_trading_dates)}")
    print("\n--- Script Execution Completed ---")
    logging.info("\n--- Script Execution Completed ---")

if __name__ == "__main__":
    main()
