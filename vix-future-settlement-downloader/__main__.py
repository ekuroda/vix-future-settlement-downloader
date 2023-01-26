# dateからdate-toの日付まで(date-toを省略した場合dateの日付のみ)の各日について、
# 1〜8ヶ月先物の期日,価格を e1,p1,e2,p2,...,e8,p8 フィールド値としてcsv出力する。
# 出力さきはfileオプションで指定する。指定しなければカレントディレクトリのout.csvに出力。
#
# 例:
# python -m vix-future-settlement-downloader --date 2023-1-24 --date-to 2023-1-25 --file 20220124-0125.csv
#
# なお、download元の制約により最近3ヶ月程度のデータしか正しく取得できない。

import argparse
import pandas as pd
from datetime import timedelta, datetime
from logging import getLogger, StreamHandler, DEBUG


logger = getLogger(__name__)
logger.setLevel(DEBUG)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.addHandler(handler)

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--date-to', type=str)
parser.add_argument('--file', type=str)


def main():
    args = parser.parse_args()

    if args.date == None:
        logger.error('date required')
        exit(1)

    date = datetime.strptime(args.date, '%Y-%m-%d')
    date_to = datetime.strptime(args.date_to, '%Y-%m-%d') if args.date_to is not None else None

    logger.info(f'date: {date}, date_to: {date_to}')

    if date_to is None:
        date_to = date 

    if date < date_to:
        step = 1
        is_done = lambda curr: curr > date_to
    else:
        step = -1
        is_done = lambda curr: curr < date_to

    result = pd.DataFrame({'e1':[], 'p1':[], 'e2':[], 'p2':[], 'e3':[], 'p3':[], 'e4':[], 'p4':[], 'e5':[], 'p5':[],'e6':[], 'p6':[],'e7':[], 'p7':[],'e8':[], 'p8':[],})

    def next(date, step):
        curr = date
        while True:
            yield curr
            curr += timedelta(days=step)
            if is_done(curr):
                break

    for d in next(date, step):
        dt = d.strftime('%Y-%m-%d')
        url = f'https://www.cboe.com/us/futures/market_statistics/settlement/csv?dt={dt}'
        try:
            df = pd.read_csv(url)
        except Exception as e:
            logger.warning(f'{dt}: {e}')
            break

        df = df[df.Product == 'VX']
        month1 = ['VX/' in x for x in df.Symbol]
        df = df[month1]

        s = pd.Series(data={}, dtype=object)
        if df.shape[0] == 0:
            logger.info(f'{dt}: {list(s)}')
            result.loc[d] = s
            continue

        df.index = pd.to_datetime(df['Expiration Date'])
        df = df.sort_index()
        #print(df)

        month = 1
        for row in df.itertuples():
            s[f'e{month}'] = row[3]
            s[f'p{month}'] = row[4]
            month += 1
            if month > 8:
                break

        logger.info(f'{dt}: {list(s)}')
        result.loc[d] = s

    path = args.file if args.file is not None else 'out.csv'
    with open(path, 'w') as f:
        f.write(result.to_csv())


if __name__ == '__main__':
    main()
