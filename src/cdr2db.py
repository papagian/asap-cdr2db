import argparse
import datetime
import os
import sys

from progress.spinner import Spinner

from sqlalchemy import create_engine, Table, Column, Integer, MetaData, String, DateTime

def prepate_db(conn_str):
     engine = create_engine(conn_str)
     metadata = MetaData()
     t = Table('cdr',
               metadata,
               Column('cdrType', String),
               Column('callingPartyNumberKey', String),
               Column('callingSubscriberImsi', String),
               Column('dateTime', DateTime),
               Column('chargeableDuration', Integer),
               Column('exchangeIdentity', String),
               Column('outgoingRoute', String),
               Column('incomingRoute', String),
               Column('cellId1stCellCalling', String),
               Column('gsmTeleServiceCode', String),
               Column('cellIdLastCellCalling', String),
               Column('disconnectingParty', String),
               Column('callingSubscriberImei', String),
               Column('tac', String),
               Column('residentCustomerFlag', String),
               Column('paymentType', String),
               Column('contractStatus', String),
               Column('contractStartingDate', String),
               Column('country', String),
               Column('cityPostalCode', String),
               Column('city', String))

     metadata.create_all(engine)
     return engine.connect(), t


def extract_cdr_data(f):
    spinner = Spinner('Loading %s:' % f)
    with open(f) as fd:
        while 1:
            l = fd.readline()
            spinner.next()
            if not l:
                raise StopIteration
            if l == '':
                continue
            t = l.split(';')
            try:
                d = datetime.datetime.strptime(':'.join([t[3], t[4]]),
                                            '%Y%m%d:%H%M%S')
            except:
                continue
            else:
                yield {
                    'cdrType': t[0],
                    'callingPartyNumberKey': t[1],
                    'callingSubscriberImsi': t[2],
                    'dateTime': d,
                    'chargeableDuration': int(t[5]),
                    'exchangeIdentity': t[6],
                    'outgoingRoute': t[7],
                    'incomingRoute': t[8],
                    'cellId1stCellCalling': t[9],
                    'gsmTeleServiceCode': t[10],
                    'cellIdLastCellCalling': t[11],
                    'disconnectingParty': t[12],
                    'callingSubscriberImei': t[13],
                    'tac': t[14],
                    'residentCustomerFlag': t[15],
                    'paymentType': t[16],
                    'contractStatus': t[17],
                    'contractStartingDate': t[18],
                    'country': t[19],
                    'cityPostalCode': t[20],
                    'city': t[21]
                }


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
