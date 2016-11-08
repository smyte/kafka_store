#!/usr/bin/env python3
import fastavro
import sys

def reader(file_object):
    return fastavro.reader(file_object)

def read_file(filename):
    with open(filename, 'rb') as fo:
        for record in reader(fo):
            yield record
