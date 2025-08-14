import os
import argparse
import datetime
import json

import config

import utils


VAR_PREC = 'total_precipitation'
VAR_TEMP = '2m_temperature'


VALID_VARIABLES = {VAR_PREC, VAR_TEMP}

VAR_FOLDERNAME = {
    VAR_PREC: config.DATA_PREC_FOLDERPATH,
    VAR_TEMP: config.DATA_TEMP_FOLDERPATH
}

VAR_PREFIX = {
    VAR_PREC: 'total-precipitation',
    VAR_TEMP: '2m-temperature',
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog = 'python scripts/download_single.py',
        description = (
            'Script to ERA5 data daily single values -- daily mean temperature, daily total precipitation'
        ),
    )

    parser.add_argument('-c', '--credentials', action='store', required=True, help="filepath to json file with CDSAPI credentials")
    parser.add_argument('-d', '--date', action='store', required=True, help="[YYYY-MM-DD] Date to download the variable for.")
    parser.add_argument('-v', '--var', action='store', required=True, help="[options: '2m_temperature' 'total_precipitation'] ERA5 variable to download.")
    parser.add_argument('-f', '--filepath', action='store', default=None, required=False, help="filepath where the file is to be downloaded to. Overrides -F.")
    parser.add_argument('-F', '--folderpath', action='store', default=None, required=False, help="folderpath where the file is to be downloaded to. Overriden by -f.")
    parser.add_argument('-m', '--request-full-month', action='store_true', help="Download full month.")

    args = parser.parse_args()

    with open(args.credentials, 'r') as h:
        credentials = json.load(h)
    
    URL = credentials['url']
    KEY = credentials['key']

    date = datetime.datetime.strptime(args.date, '%Y-%m-%d')

    download_filepath = args.filepath

    if download_filepath is None:
        download_folderpath = args.folderpath

        if download_folderpath is None:
            download_folderpath = VAR_FOLDERNAME[args.var]

        download_filepath = os.path.join(
            download_folderpath, 
            f"{VAR_PREFIX[args.var]}_{date.strftime('%Y-%m-%d')}.nc",
        )

    if args.var not in VALID_VARIABLES:
        raise ValueError(f'Invaild var={args.var}')

    if args.var == VAR_PREC:
        utils.download_total_precipitation(
            date = date,
            download_filepath = download_filepath,
            url = URL,
            key = KEY,
            request_full_month = args.request_full_month,
        )
    elif args.var == VAR_TEMP:
        utils.download_total_precipitation(
            date = date,
            download_filepath = download_filepath,
            url = URL,
            key = KEY,
            request_full_month = args.request_full_month,
        )
    else:
        raise NotImplementedError(f'var={args.var} not implemented.')
