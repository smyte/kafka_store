#!/usr/bin/env python3
import fastavro
import sys

def reader(file_object):
    return fastavro.reader(file_object)

def read_file(filename):
    with open(filename, 'rb') as fo:
        for record in reader(fo):
            yield record

if __name__ == '__main__':
    count = 0
    for record in read_file(sys.argv[1]):
        print(record)
        count += 1
    print('Read', count, 'messages')
