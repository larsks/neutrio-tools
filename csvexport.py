#!/usr/bin/env python

import argparse
import calendar
import csv
import datetime
import neurio

import creds

fieldnames = [
    'timestamp',
    'netEnergy',
    'netPower',
    'generationEnergy',
    'generationPower',
    'consumptionEnergy',
    'consumptionPower',
]


def DateArg(value):
    year, month, day = (int(x) for x in value.split('-', 2))
    return datetime.datetime(year=year, month=month, day=day)


def parse_args():
    p = argparse.ArgumentParser()

    p.add_argument('--granularity', '-g', default='days',
                   choices=['years', 'weeks', 'minutes',
                            'months', 'days', 'hours'])
    p.add_argument('--output', '-o', default='samples.csv')
    p.add_argument('--page-size', '-p', default=300, type=int)
    p.add_argument('--day', action='store_const',
                   const='day', dest='duration')
    p.add_argument('--week', action='store_const',
                   const='week', dest='duration')
    p.add_argument('--month', action='store_const',
                   const='month', dest='duration')
    p.add_argument('--year', action='store_const',
                   const='year', dest='duration')
    p.add_argument('period_start', type=DateArg)

    p.set_defaults(duration='day')

    return p.parse_args()


def get_all_samples(nc, writer, args):

    period_start = args.period_start

    if args.duration == 'day':
        period_end = period_start.replace(
            hour=23,
            minute=59,
            second=59,
        )
    elif args.duration == 'month':
        period_end = args.period_start.replace(
            day=calendar.monthrange(period_start.year, period_start.month)[1],
            hour=23,
            minute=59,
            second=59,
        )

    page = 1

    while True:
        samples = nc.get_samples(sensor_id=creds.sensor_id,
                                 start=period_start.isoformat(),
                                 end=period_end.isoformat(),
                                 granularity=args.granularity,
                                 per_page=args.page_size,
                                 page=page,
                                 )

        if isinstance(samples, dict) and 'status' in samples:
            raise ValueError(samples)

        if not samples:
            break

        for sample in samples:
            writer.writerow(sample)

        page += 1


def main():
    args = parse_args()

    tp = neurio.TokenProvider(key=creds.client_id,
                              secret=creds.client_secret)
    nc = neurio.Client(token_provider=tp)

    with open(args.output, 'w') as fd:
        writer = csv.DictWriter(fd, fieldnames=fieldnames)
        writer.writeheader()

        get_all_samples(nc, writer, args)

if __name__ == '__main__':
    main()
