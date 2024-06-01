import csv

def file_writer(filepath:str, writeMode:str='w'):
    writer = csv.writer(open(filepath, writeMode))
    return writer
