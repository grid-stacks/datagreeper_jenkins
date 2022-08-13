#! /usr/bin/env python3

"""Script for calculating the average time taken for a pipeline to complete"""
import argparse
import csv
import datetime
import json
import os
import random
import sys
from distutils.util import strtobool
from typing import List

import gspread
import requests
from dotenv import load_dotenv

load_dotenv()

DATAGREPPER_URL = os.getenv("DATAGREPPER_URL")
GSHEET_NAME = os.getenv("GSHEET_NAME")
PROD_ENVIRONMENT = bool(strtobool(os.getenv("PROD_ENVIRONMENT", 0)))

TOPIC_PREFIX = (
    "/topic/VirtualTopic.eng.ci.redhat-container-image.pipeline"
)

BEHAVIOUR = {
    "aborted": {
        "file_name": "aborted_final_json.json",
        "status": "aborted"
    },
    "successful": {
        "file_name": "successful_final_json.json",
        "status": "complete"
    },
    "unstable": {
        "file_name": "unstable_final_json.json",
        "status": "unstable"
    },
    "error": {
        "file_name": "error_final_json.json",
        "status": "error"
    },
}
CI = ["cvp-redhat-operator-bundle-image-validation-test", "cvp-product-test"]


def query_datagrepper(
        start_time: str, end_time: str, page: int, topic_status: str
):
    """
    Loads data from datagrepper
    :param start_time: start time
    :param end_time: end time
    :param page: page
    :param topic_status: topic status
    :return: json
    """
    payload = {
        'start': start_time,
        'end': end_time,
        'rows_per_page': 100,  # rows_per_page must be <= 100\n
        'page': page,
        'topic': f'{TOPIC_PREFIX}.{topic_status}'
    }

    data = requests.get(DATAGREPPER_URL, verify="datagrip/cert", params=payload)
    return data.json()


def get_timestamps_for_all_nvrs(start_time: str, end_time: str, status_pattern: str, ci_name_pattern: str,
                                topic_status: str, set_str: str, result: dict) -> dict:
    """
    Get timestamps for all nvrs
    :param start_time: start time
    :param end_time: end time
    :param status_pattern: status pattern
    :param ci_name_pattern: ci name pattern
    :param topic_status: topic status
    :param set_str: set string
    :param result: result dict
    :return: result dict
    """
    page = 1
    # print(start_time, end_time)
    j = query_datagrepper(start_time, end_time, page, topic_status)
    while len(j['raw_messages']) > 0:
        for one_json in j['raw_messages']:
            if ci_name_pattern in one_json['headers']['CI_NAME']:
                if status_pattern in one_json['msg']['pipeline']['status']:
                    nvr = one_json['msg']['artifact']['nvr']

                    if set_str == "end":
                        if nvr not in result:
                            result[nvr] = {'start': None, 'end': None}

                        result[nvr][set_str] = \
                            datetime.datetime.strptime(
                                one_json['msg']['timestamp'],
                                '%Y-%m-%dT%H:%M:%S.%fZ'
                            )
                    print(nvr, nvr in result)
                    if set_str == "start" and nvr in result and "start" in result[nvr].keys():
                        result[nvr][set_str] = \
                            datetime.datetime.strptime(
                                one_json['msg']['timestamp'],
                                '%Y-%m-%dT%H:%M:%S.%fZ'
                            )
                        print(result[nvr])
        page += 1
        # Extracting the data for page 2 and so on
        j = query_datagrepper(start_time, end_time, page, topic_status)

    return result


def get_avg_and_count(result: dict):
    """
    Calculate average time taken for a pipeline to complete
    :param result: result dict
    :return: average time taken
    """
    count = 0
    sum_ = 0
    for nvr in result.keys():
        if result[nvr]['start'] and result[nvr]['end']:
            result[nvr]['time_taken'] = (
                    result[nvr]['end'] - result[nvr]['start']
            )
            sum_ += result[nvr]['time_taken'].seconds
            count += 1

    if count >= 1:
        avg_ = sum_ / (count * 60)
    else:
        avg_ = 0

    return avg_, count


def output_csv(result: dict):
    """
    New result dict
    :param result: result dict
    :return: new result dict
    """
    field_names = ['NVR', 'Status of the Pipeline', 'Duration']
    with open('Spreadsheet1.csv', 'w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(field_names)
        for nvr in result.keys():
            if result[nvr]['time_taken']:
                writer.writerow(
                    [nvr, 'Complete', result[nvr]["time_taken"]])


def find_weeks(start_time, end_time) -> List[tuple]:
    """
    Find the weeks between start_time and end_time
    :param start_time: start time
    :param end_time: end time
    :return: list of weeks
    """
    # convert start_time and end_time to datetime objects
    weeks = []
    while start_time <= end_time:
        if start_time + datetime.timedelta(days=6) < end_time:
            weeks.append((
                start_time.strftime('%s'),
                (start_time + datetime.timedelta(days=6)).strftime('%s')
            ))
        else:
            if start_time == end_time:
                weeks.append((
                    start_time.strftime('%s'),
                    (end_time +
                     datetime.timedelta(
                         hours=23,
                         minutes=59,
                         seconds=59)).strftime('%s')
                ))
            else:
                weeks.append((
                    start_time.strftime('%s'),
                    end_time.strftime('%s')
                ))
        start_time += datetime.timedelta(days=7)

    return weeks


# genrates a json file containing week as a key and values
# as avg of the pipelines in that week and count of pipelines
# in that week
def generate_weekly_json_data(start_time, end_time, status, ci, file_name):
    """
    Generate final json
    :param start_time: start time
    :param end_time: end time
    :param status: nvr status
    :param ci: nvr ci name
    :param file_name: json file name to export data
    """
    # get the start and end time of the week between start_time and end_time
    weeks = find_weeks(start_time, end_time)  # [(s1, e1), (s2, e2)]
    print(f"\t\t\t[weeks] {weeks}")

    final_json = {}

    for week in weeks:
        if PROD_ENVIRONMENT:
            output = get_timestamps_for_all_nvrs(
                week[0],
                week[1],
                status,
                ci,
                'complete', 'end', {})

            output = get_timestamps_for_all_nvrs(
                week[0],
                week[1],
                'running',
                ci,
                'running', 'start', output)

            avg, count = get_avg_and_count(output)
        else:
            avg = random.uniform(1, 10000)
            count = random.randint(1, 1000)

        start = datetime.datetime.fromtimestamp(
            int(week[0])).strftime('%Y%m%d')
        end = datetime.datetime.fromtimestamp(
            int(week[1])).strftime('%Y%m%d')
        final_json[f"{start}-{end}"] = {
            'avg': avg,
            'count': count,
            'time_string': format_seconds(avg),
            'ci': ci
        }  # {ws1-we1: {avg:, count:}}

        print(f"\t\t\t[json] {final_json}")

    with open(file_name, 'w') as f:
        json.dump(final_json, f)

    return final_json


def format_seconds(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{h:.2f} h {m:.2f} min {s:.2f} sec"


def week_ago_date(start_time, end_time):
    week_ago_start_time = start_time - datetime.timedelta(days=7)
    week_ago_end_time = start_time - datetime.timedelta(days=1)
    return start_time, end_time, week_ago_start_time, week_ago_end_time


def validate_date(string):
    """
    Convert input string to a date, or error.

    https://stackoverflow.com/questions/25470844/specify-format-for-input-arguments-argparse-python
    """
    try:
        return datetime.datetime.strptime(string, '%Y-%m-%d').date()
    except ValueError:
        msg = f"Not a valid date: '{string}'."
        raise argparse.ArgumentTypeError(msg)


def pipeline_generator(pipeline, current, ago):
    pipeline.update({ci: {"count_current": current[list(current.keys())[0]]["count"]}})
    pipeline[ci]["count_ago"] = ago[list(ago.keys())[0]]["count"]
    pipeline[ci]["avg_current"] = current[list(current.keys())[0]]["avg"]
    pipeline[ci]["avg_ago"] = ago[list(ago.keys())[0]]["avg"]
    pipeline[ci]["status"] = status
    pipeline[ci]["ci"] = ci


def ago_stats(result_list, total_pipelines, pipeline, ci, status):
    result_list.append("")
    pc = (pipeline[ci]["count_ago"] / total_pipelines) * 100
    result_list.append(
        f"Triggered Red Hat {status} container pipelines: {pipeline[ci]['count_ago']} {pc:.2f}%")
    result_list.append(f"Average Duration: {format_seconds(pipeline[ci]['avg_ago'])}")


def current_stats(result_list, total_pipelines, pipeline, ci, status):
    result_list.append("")
    pc = (pipeline[ci]["count_current"] / total_pipelines) * 100
    count_pc = (((pipeline[ci]["count_ago"] - pipeline[ci]["count_current"]) / pipeline[ci]["count_ago"]) * 100) if \
        pipeline[ci]["count_ago"] else 0
    avg_change = pipeline[ci]["avg_ago"] - pipeline[ci]["avg_current"]
    result_list.append(
        f"Triggered Red Hat {status} container pipelines: {pipeline[ci]['count_current']} {pc:.2f}% ({count_pc:.2f}% WtW)")
    result_list.append(
        f"Average Duration: {format_seconds(pipeline[ci]['avg_current'])} ({format_seconds(avg_change)} WtW)")


def write_gsheet(current_time, result_list):
    service_account = gspread.service_account(filename="service_account.json")
    gsheet = service_account.open(GSHEET_NAME)

    try:
        gsheet.add_worksheet(title=current_time, rows=len(result_list), cols=1)
    except gspread.exceptions.APIError as e:
        pass

    worksheet = None

    try:
        worksheet = gsheet.worksheet(title=current_time)
    except gspread.exceptions.WorksheetNotFound as e:
        pass

    if worksheet:
        print("==================================================")
        print(f"[gsheet] Start writing. Total {len(result_list)} rows.")

        cell_list = worksheet.range(f"A1:A{len(result_list)}")
        for index, cell in enumerate(cell_list):
            print(result_list[index])
            cell.value = result_list[index]

        worksheet.update_cells(cell_list)

        print("[gsheet] writing completed")


if __name__ == '__main__':
    # take the start and end time as arguments from the cmd line
    start_time = validate_date(sys.argv[1])
    end_time = validate_date(sys.argv[2])

    current_start_time, current_end_time, ago_start_time, ago_end_time = week_ago_date(start_time, end_time)
    print(current_start_time, current_end_time, ago_start_time, ago_end_time)

    # Making directory for current run
    current_time = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    current_dir = os.getcwd()
    path = os.path.join(current_dir, current_time)
    os.mkdir(path)

    pipeline_aborted = {}
    pipeline_successful = {}
    pipeline_unstable = {}
    pipeline_error = {}

    for status in BEHAVIOUR.keys():
        print("==================================================")
        print(f"[status] running for {BEHAVIOUR[status]['status']}")

        file_name = BEHAVIOUR[status]["file_name"]

        for index, ci in enumerate(CI):
            print(f"\t[ci] running for {ci}")

            print(f"\t\t[ago] running for {ago_start_time} - {ago_end_time}")
            ago = generate_weekly_json_data(start_time=ago_start_time, end_time=ago_end_time,
                                            status=BEHAVIOUR[status]['status'], ci=ci,
                                            file_name=f"{path}/ago_{index}_{file_name}")

            print(f"\t[current] running for {current_start_time} - {current_end_time}")
            current = generate_weekly_json_data(start_time=current_start_time, end_time=current_end_time,
                                                status=BEHAVIOUR[status]['status'],
                                                ci=ci,
                                                file_name=f"{path}/current_{index}_{file_name}")

            if status == "aborted":
                pipeline_generator(pipeline_aborted, current, ago)
                print("\t\t[aborted output]", pipeline_aborted)
            elif status == "successful":
                pipeline_generator(pipeline_successful, current, ago)
                print("\t\t[successful output]", pipeline_successful)
            elif status == "unstable":
                pipeline_generator(pipeline_unstable, current, ago)
                print("\t\t[unstable output]", pipeline_unstable)
            elif status == "error":
                pipeline_generator(pipeline_error, current, ago)
                print("\t\t[error output]", pipeline_error)

    result_list = [f"Date {ago_start_time} to {ago_end_time}", "Results:", "Pipeline stats:"]

    for ci in CI:
        result_list.append(f"CI name: {ci}")
        total_pipelines = pipeline_aborted[ci]["count_ago"] + pipeline_successful[ci]["count_ago"] + \
                          pipeline_unstable[ci]["count_ago"] + pipeline_error[ci]["count_ago"]

        if total_pipelines:
            result_list.append(f"Triggered Red Hat container pipelines: {total_pipelines}")

            ago_stats(result_list, total_pipelines, pipeline_successful, ci, "successful")
            ago_stats(result_list, total_pipelines, pipeline_aborted, ci, "aborted")
            ago_stats(result_list, total_pipelines, pipeline_unstable, ci, "unstable")
            ago_stats(result_list, total_pipelines, pipeline_error, ci, "error")

            result_list.append("")

    result_list.append("")

    result_list.append(f"Date {current_start_time} to {current_end_time}")
    result_list.append("Results:")
    result_list.append("Pipeline stats:")

    for ci in CI:
        result_list.append(f"CI name: {ci}")
        total_pipelines = pipeline_aborted[ci]["count_current"] + pipeline_successful[ci]["count_current"] + \
                          pipeline_unstable[ci]["count_current"] + pipeline_error[ci]["count_current"]

        if total_pipelines:
            result_list.append(f"Triggered Red Hat container pipelines: {total_pipelines}")

            current_stats(result_list, total_pipelines, pipeline_successful, ci, "successful")
            current_stats(result_list, total_pipelines, pipeline_aborted, ci, "aborted")
            current_stats(result_list, total_pipelines, pipeline_unstable, ci, "unstable")
            current_stats(result_list, total_pipelines, pipeline_error, ci, "error")

            result_list.append("")

    write_gsheet(current_time, result_list)
