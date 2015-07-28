import argparse
import datetime
import os
import sys

from progress.spinner import Spinner

from sqlalchemy import create_engine, Table, Column, Integer, MetaData, String

def prepate_db(conn_str):
     engine = create_engine(conn_str)
     metadata = MetaData()
     t = Table('cdr', metadata,
                 Column('id', String),
                 Column('hour', Integer),
                 Column('dow', Integer),
                 Column('doy', Integer))
     metadata.create_all(engine)
     return engine.connect(), t


def extract_cdr_data(f):
    spinner = Spinner('Loading %s:' % f)
    with open(f) as fd:
        fd.readline()  # omit the headline
        while 1:
            l = fd.readline()
            spinner.next()
            if not l:
                raise StopIteration
            if l == '':
                continue
            separator = ';'  # new CDR formt
            t = l.split(separator)
            d = datetime.datetime.strptime(':'.join([t[3], t[4]]),  # new CDR format
                                           '%Y%m%d:%H%M%S')
            dt = d.timetuple()
            yield {'id': t[9],  # new CDR format
                   'hour': dt.tm_hour,
                   'dow': (dt.tm_wday + 2) % 7, # normalize to conform java.util.Calendar
                   'doy': dt.tm_yday}


def traverse_cdr(path, conn, insert_stmt):
    if not os.path.exists(path):
        raise Exception('Please provide a valid path!')
    if os.path.isfile(path):
        for d in extract_cdr_data(path):
            conn.execute(insert_stmt, **d)
    for root, dirs, files in os.walk(args.cdr_path):
        for d in dirs:
            traverse_cdr(d)
        for f in files:
            for d in extract_cdr_data(os.path.join(root, f)):
                conn.execute(insert_stmt, **d)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("conn_str",
                        type=str,
                        help="""the database URL formatted as:
                                dialect+driver://user:password@host/dbname[?key=value..]
                                see also:
                                http://docs.sqlalchemy.org/en/rel_0_7/core/engines.html#database-urls""")
    parser.add_argument("cdr_path",
                        type=str,
                        help="""path (file or directory) pointing to the CDR data""")
    args = parser.parse_args()

    conn, table = prepate_db(args.conn_str)
    insert_stmt = table.insert()
    try:
        values = traverse_cdr(args.cdr_path, conn, insert_stmt)
    except Exception, e:
        sys.stderr.write(unicode(e))
