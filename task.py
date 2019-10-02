#!/usr/bin/env python3
import os
import json
import lzma
import tarfile
import urllib.request
from urllib.error import URLError

import pandas as pd

url_path = 'https://collector.torproject.org/archive/exit-lists/'
path_to_extract = '/tmp/exit-nodes'
files = ['exit-list-2019-07.tar.xz', 'exit-list-2019-08.tar.xz']
months = ('July', 'August', 'Both')


def unpack_archive(file):
    try:
        with lzma.open(file) as f:
            with tarfile.open(fileobj=f) as tar:
                tar.extractall(path_to_extract)
    except FileNotFoundError:
        pass


def read_file(file):
    lines = list()
    try:
        with open(file) as f:
            next(f)
            next(f)  # skip 2 header lines
            for line in f:
                title, data = line.strip().split(' ', 1)
                if title == 'ExitAddress':
                    lines.append(data.split(' ', 1))  # split into IP address and date
    except FileNotFoundError:
        pass
    return lines


def parse_files(list_of_files):
    nodes_list = list()
    res = dict()
    for file in list_of_files:
        lines = read_file(file)
        nodes_list.extend(lines)

    titles = ('ExitAddress', 'AddressDate')
    df = pd.DataFrame(nodes_list, columns=titles)
    if df.empty:
        return res

    gr = df.groupby(['ExitAddress']).size().reset_index(name='Count')
    ip = gr.sort_values(by='Count', ascending=False)['ExitAddress'].iloc[0]  # most frequent IP address

    filtered_by_ip = df.loc[df['ExitAddress'] == ip].sort_values(by='AddressDate')
    date_first = filtered_by_ip['AddressDate'].iloc[0]  # first seen
    date_last = filtered_by_ip['AddressDate'].iloc[-1]  # last seen

    unique_ips = df['ExitAddress'].unique().tolist()  # for calculating difference

    res['ip'] = ip
    res['first_seen'] = date_first
    res['last_seen'] = date_last
    res['unique_ips'] = unique_ips

    return res


def get_all_files():
    results = list()
    list_of_files = list()
    for file in files:
        list_of_files.append([])
        dest_dir = file.split('.', 1)[0]  # archive name without extension
        for (dirpath, dirnames, filenames) in os.walk(os.path.join(path_to_extract, dest_dir)):
            for filename in filenames:
                list_of_files[-1].append(os.path.join(path_to_extract, dest_dir, dirpath, filename))  # full file path

    for files_for_month in list_of_files:
        parsing = parse_files(files_for_month)  # calculate separate results for months
        if parsing:
            results.append(parsing)

    if len(results) == len(files):  # if results exist for both months
        first_ips, second_ips = [results[i].get('unique_ips') for i in (0, 1)]
        results[0]['unique_ips'] = difference(first_ips, second_ips)
        results[1]['unique_ips'] = difference(second_ips, first_ips)

        if results[0]['ip'] == results[1]['ip']:  # both months have the same most frequent IP
            # Therefore take first_seen from the 1st month, last_seen from the 2nd month
            results.append({'ip': results[0]['ip'], 'first_seen': results[0]['first_seen'],
                            'last_seen': results[1]['last_seen'], 'unique_ips': []})
        else:
            # Calculate results for both months together
            parsing = parse_files(sum(list_of_files, []))
            if parsing:
                results.append(parsing)
                results[-1]['unique_ips'] = []

    write_result(results)


def write_result(results):
    data = dict()
    if len(results) == len(months):
        for i, month in enumerate(months):
            data[month] = results[i]

    json_data = json.dumps(data)
    with open('results.json', 'w') as f:
        f.write(json_data)


def difference(first, second):  # find difference first - second
    second = set(second)
    return [el for el in first if el not in second]


if __name__ == '__main__':
    try:
        os.makedirs(path_to_extract)
    except FileExistsError:
        pass

    if not os.path.isdir(path_to_extract):
        print(f"{path_to_extract} is not a directory")
        exit()

    for archive in files:
        try:
            urllib.request.urlretrieve(os.path.join(url_path, archive), os.path.join(path_to_extract, archive))
        except URLError:  # proceed even if file wasn't downloaded
            print(f"Error while retrieving a file {archive}")
        unpack_archive(os.path.join(path_to_extract, archive))
    get_all_files()
