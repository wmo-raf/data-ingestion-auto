from datetime import datetime, timedelta
from dateutil import relativedelta


def get_next_month_date(iso_date_string):
    date_obj = datetime.fromisoformat(iso_date_string)
    year, month = date_obj.year, date_obj.month

    if month == 12:
        year += 1
        month = 1
    else:
        month += 1

    return datetime(year, month, 1)


def get_next_pentad(iso_date):
    date = datetime.fromisoformat(iso_date)
    day = date.day

    if day <= 5:
        next_pentad_start = datetime(date.year, date.month, 6)
        next_pentad_num = 2
    elif day <= 10:
        next_pentad_start = datetime(date.year, date.month, 11)
        next_pentad_num = 3
    elif day <= 15:
        next_pentad_start = datetime(date.year, date.month, 16)
        next_pentad_num = 4
    elif day <= 20:
        next_pentad_start = datetime(date.year, date.month, 21)
        next_pentad_num = 5
    elif day <= 25:
        next_pentad_start = datetime(date.year, date.month, 26)
        next_pentad_num = 6
    else:
        next_pentad_start = date + relativedelta.relativedelta(months=1, day=1)
        next_pentad_num = 1

    return next_pentad_start, next_pentad_num
