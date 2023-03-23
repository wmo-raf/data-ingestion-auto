from datetime import datetime


def get_next_month_date(iso_date_string):
    date_obj = datetime.fromisoformat(iso_date_string)
    year, month = date_obj.year, date_obj.month

    if month == 12:
        year += 1
        month = 1
    else:
        month += 1

    return datetime(year, month, 1)
