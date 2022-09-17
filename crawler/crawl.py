from code import interact
from collections import namedtuple
from datetime import datetime, timedelta
import json
from multiprocessing import Pool
import os
import random
import sys
from time import sleep
import time
from typing import NamedTuple
import requests
from requests_ratelimiter import LimiterSession

GHPAT = os.environ.get('GHPAT')

headers = {
    "Accept": "application/vnd.github.v3+json",
    "Authorization": f"token {GHPAT}"
}

sess = LimiterSession(per_minute=30)

min_stars = 10


def is_day_mode(time: datetime):
    return time.strftime("%H:%M:%S") == '00:00:00'


def get_with_retry(*args, **kwarg):
    for i in range(3):
        res = sess.get(*args, **kwarg)
        if res.status_code == 200:
            return res
        print(
            f'Get {res.url} failed: {res.status_code}, Retrying...({i + 1}/3)', file=sys.stderr)


def get_repo_count(start: datetime, end: datetime):
    if is_day_mode(start) and is_day_mode(end):
        start_time = start.strftime('%Y-%m-%d')
        end_time = end.strftime('%Y-%m-%d')
    else:
        start_time = start.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time = end.strftime('%Y-%m-%dT%H:%M:%SZ')
    res = get_with_retry("https://api.github.com/search/repositories", params={
        'q': f'language:Rust stars:>={min_stars} created:{start_time}..{end_time}',
        'per_page': 100,
    }, headers=headers)
    return res.json().get('total_count')


def get_repo_list(start: datetime, end: datetime) -> list:
    if is_day_mode(start) and is_day_mode(end):
        start_time = start.strftime('%Y-%m-%d')
        end_time = end.strftime('%Y-%m-%d')
    else:
        start_time = start.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time = end.strftime('%Y-%m-%dT%H:%M:%SZ')
    items = []
    page = 1
    while True:
        res = get_with_retry("https://api.github.com/search/repositories", params={
            'sort': 'stars',
            'order': 'desc',
            'q': f"language:Rust stars:>={min_stars} created:{start_time}..{end_time}",
            'per_page': 100,
            'page': page,
        }, headers=headers)
        json = res.json()
        items.extend(json.get('items'))
        if int(json.get('total_count')) <= page * 100:
            break
        page += 1
    return items


class Span(NamedTuple):
    '''
    Including both ends.
    '''
    start: datetime
    end: datetime
    count: int


def save(file: str, data: list[Span]):
    with open(file, 'w') as f:
        for span in data:
            f.write(
                f'{span.start.strftime("%Y-%m-%dT%H:%M:%SZ")} {span.end.strftime("%Y-%m-%dT%H:%M:%SZ")} {span.count}\n')


def load(file: str) -> list[Span]:
    with open(file, 'r') as f:
        res = []
        for line in f:
            start, end, count = line.strip().split()
            start = datetime.strptime(start, '%Y-%m-%dT%H:%M:%SZ')
            end = datetime.strptime(end, '%Y-%m-%dT%H:%M:%SZ')
            count = int(count) if count != 'None' else get_repo_count(
                start, end)
            res.append(Span(start, end, count))
        return res


def split(span: Span) -> list[Span]:
    start = span.start
    end = span.end
    if start == end:
        mid = start + timedelta(hours=12)
        mid1 = mid + timedelta(seconds=1)
        end = start + timedelta(hours=23, minutes=59, seconds=59)
        return [Span(start, mid, get_repo_count(start, mid)),
                Span(mid1, end, get_repo_count(mid1, end))]
    else:
        days = (end - start).days
        mid = start + timedelta(days=days // 2)
        mid1 = mid + timedelta(days=1)
        return [Span(start, mid, get_repo_count(start, mid)),
                Span(mid1, end, get_repo_count(mid1, end))]


def merge(data: list[Span]) -> Span:
    start = data[0].start
    end = data[-1].end
    count = sum(span.count for span in data)
    return Span(start, end, count)


def main():
    wd = "crawl-10star-new"
    os.makedirs(wd, exist_ok=True)
    os.chdir(wd)

    # Generate timespans.txt
    if os.path.exists('timespans_splitted.txt'):
        timespans = load("timespans_splitted.txt")
    elif os.path.exists('timespans.txt'):
        timespans = load("timespans.txt")
    else:
        timespans = []

    if not timespans:
        start = datetime(2010, 1, 1)
        while start < datetime.now():
            end = start + timedelta(days=30)
            start_time = start.strftime('%Y-%m-%d')
            end_time = end.strftime('%Y-%m-%d')
            count = get_repo_count(start, end)
            print(f"{start_time} {end_time} {count}")
            timespans.append(Span(start, end, count))
            start = end + timedelta(days=1)
        save("timespans.txt", timespans)
    else:
        print(f"Loaded {len(timespans)} timespans from timespans.txt")
        for span in timespans:
            if span.count is None:
                span.count = get_repo_count(span.start, span.end)
                print(f"{span.start} {span.end} {span.count}")

    # split big spans
    result = []
    for span in timespans:
        if span.count > 1000:
            print(f"Splitting {span}")
            result.extend(split(span))
        else:
            result.append(span)
    timespans = result

    save("timespans_splitted.txt", timespans)

    # Check
    for span in timespans:
        if span.count > 1000:
            print("Failed to split span, try again", file=sys.stderr)
            exit(0)

    # merge small spans
    result = []
    accu_count = 0
    begin = 0
    for i in range(len(timespans)):
        span = timespans[i]
        if accu_count + span.count > 1000:
            result.append(merge(timespans[begin:i]))
            begin = i
            accu_count = span.count
        else:
            accu_count += span.count
    if begin < len(timespans):
        result.append(merge(timespans[begin:]))
    timespans = result

    save("timespans_merged.txt", timespans)

    def get_and_save(path: str, span: Span):
        with open(path, 'w') as f:
            items = get_repo_list(span.start, span.end)
            for item in items:
                jsline = json.dumps(item)
                f.write(jsline + '\n')

    def check_and_save(span: Span):
        path = f"{span.start.strftime('%Y-%m-%dT%H:%M:%SZ')}-{span.end.strftime('%Y-%m-%dT%H:%M:%SZ')}.jsonl"
        if os.path.exists(path):
            with open(path, 'r') as f:
                count = len(f.readlines())
            if count != span.count:
                print(
                    f"Count mismatch {count} != {span.count} in {path}, fetching", file=sys.stderr)
                get_and_save(path, span)
                print(f"{span.start} {span.end} {span.count} (fetched)")
            else:
                print(f"{span.start} {span.end} {span.count} (finished)")
            return count
        else:
            get_and_save(path, span)
            print(f"{span.start} {span.end} {span.count} (fetched)")
        return span.count

    for i, span in enumerate(timespans):
        count = check_and_save(span)
        if count != span.count:
            timespans[i] = Span(span.start, span.end, count)

    save("timespans_merged.txt", timespans)


if __name__ == '__main__':
    main()
    print("Done")
    exit(0)
