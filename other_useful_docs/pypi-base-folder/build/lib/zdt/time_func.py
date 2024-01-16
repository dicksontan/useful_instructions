import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
import numpy as np
from dateutil.parser import parse
import pytz

def astimezone_sg(row):
    return row.astimezone(pytz.timezone('Singapore'))

def get_year_start(date, delta= 0):

    '''Takes in a datetime object and returns datetime object'''

    year = date.year - delta

    return dt.datetime(year,1,1).date()

def get_month_start(date, delta= 0):

    '''Takes in a datetime object and returns datetime object'''

    month = (date - relativedelta(months=delta)).month
    
    year = (date - relativedelta(months=delta)).year

    #if month <= 0:
    #    month = month + 12

    return dt.datetime(year,month,1).date()

def get_day_start(date, delta= 0):

    '''Takes in a datetime object and returns datetime object'''

    days_date = (date-dt.timedelta(days=delta))

    return days_date.date()

def get_month_end_date(date1: dt):

    '''This functions takes in any date and returns the month end date'''

    month = date1.month
    next_month = month+1
    if next_month >12:
        next_month-=12

    month_end_date = (date1.replace(day=1,month=next_month)- dt.timedelta(1)).replace(microsecond=0)

    return month_end_date

def generate_3month_time_periods(start_date,end_date):

    period_start_m_temp = [d.replace(day=1) for d in pd.date_range(start = start_date,end= end_date, freq = '3M')]

    # generating 3 month period
    # the last period end is taken out and replaced by yesterday's date

    period_end_m_temp = [d - dt.timedelta(days=1) for d in period_start_m_temp[1:]]+[pd.Timestamp(end_date)]

    period_start_m = [d.strftime('%Y%m%d') for d in period_start_m_temp]

    period_end_m = [d.strftime('%Y%m%d') for d in period_end_m_temp]

    return period_start_m, period_end_m

def generate_3month_time_periods_v2(start_date:str,end_date:str):

    # generating 3 month period
    # the last period end is taken out and replaced by yesterday's date
    # v2 version ensures that the time periods does not extend to more than 3 months
    # can see generate_monthly_time_periods_v2 description for more information

    end_date1= parse(end_date) + relativedelta(months=1)

    period_start_m_temp = [d.replace(day=1) for d in pd.date_range(start = start_date,end= end_date1, freq = '3M',inclusive='left')]

    period_end_m_temp = [d - dt.timedelta(days=1) for d in period_start_m_temp[1:]]+[pd.Timestamp(end_date)]

    period_start_m = [d.strftime('%Y%m%d') for d in period_start_m_temp]

    period_end_m = [d.strftime('%Y%m%d') for d in period_end_m_temp]

    return period_start_m, period_end_m

def generate_6month_time_periods(start_date,end_date):

    period_start_m_temp = [d.replace(day=1) for d in pd.date_range(start = start_date,end= end_date, freq = '6M')]

    # generating 3 month period
    # the last period end is taken out and replaced by yesterday's date

    period_end_m_temp = [d - dt.timedelta(days=1) for d in period_start_m_temp[1:]]+[pd.Timestamp(end_date)]

    period_start_m = [d.strftime('%Y%m%d') for d in period_start_m_temp]

    period_end_m = [d.strftime('%Y%m%d') for d in period_end_m_temp]

    return period_start_m, period_end_m

def generate_6month_time_periods_v2(start_date:str,end_date:str):

    # generating 6 month period
    # the last period end is taken out and replaced by yesterday's date
    # v2 version ensures that the time periods does not extend to more than 6 months
    # can see generate_monthly_time_periods_v2 description for more information

    end_date1= parse(end_date) + relativedelta(months=1)

    period_start_m_temp = [d.replace(day=1) for d in pd.date_range(start = start_date,end= end_date1, freq = '6M',inclusive='left')]

    period_end_m_temp = [d - dt.timedelta(days=1) for d in period_start_m_temp[1:]]+[pd.Timestamp(end_date)]

    period_start_m = [d.strftime('%Y%m%d') for d in period_start_m_temp]

    period_end_m = [d.strftime('%Y%m%d') for d in period_end_m_temp]

    return period_start_m, period_end_m

def generate_yearly_time_periods(start_date,end_date):

    period_start_m_temp = [d.replace(day=1) for d in pd.date_range(start = start_date,end= end_date, freq = 'YS')]

    # generating 3 month period
    # the last period end is taken out and replaced by yesterday's date

    period_end_m_temp = [d - dt.timedelta(days=1) for d in period_start_m_temp[1:]]+[pd.Timestamp(end_date)]

    period_start_1 = [d.strftime('%Y%m%d') for d in period_start_m_temp]

    period_end_1 = [d.strftime('%Y%m%d') for d in period_end_m_temp]

    return period_start_1, period_end_1

def generate_monthly_time_periods(start_date,end_date):

    period_start_m_temp = [d.replace(day=1) for d in pd.date_range(start = start_date,end= end_date, freq = 'M')]

    # generating period end as 1 month from each period start.
    # the last period end is taken out and replaced by yesterday's date

    period_end_m_temp = [d+ relativedelta(day=31) for d in period_start_m_temp][:-1]+[pd.Timestamp(end_date)]

    period_start_m = [d.strftime('%Y%m%d') for d in period_start_m_temp]

    period_end_m = [d.strftime('%Y%m%d') for d in period_end_m_temp]

    return period_start_m, period_end_m

def generate_monthly_time_periods_v2(start_date:str,end_date:str):

    # As compared to generate_monthly_time_periods which merges the last start period to the current date,
    # this function allows the last month to solely be the last month
    # for example, previously, if the input is '20220101' to '20220302', you will get:
    # period start: ['20220101', '20220201'] 
    # period end: ['20220131', '20220302']
    # now with v2 you get: 
    # period start v2:['20220101', '20220201', '20220301']
    # period end v2:['20220131', '20220228', '20220302']

    end_date1= parse(end_date) + relativedelta(months=1)

    period_start_m_temp = [d.replace(day=1) for d in pd.date_range(start = start_date,end= end_date1, freq = 'M',inclusive='left')]

    # generating period end as 1 month from each period start.
    # the last period end is taken out and replaced by yesterday's date

    period_end_m_temp = [d+ relativedelta(day=31) for d in period_start_m_temp][:-1]+[pd.Timestamp(end_date)]

    period_start_m = [d.strftime('%Y%m%d') for d in period_start_m_temp]

    period_end_m = [d.strftime('%Y%m%d') for d in period_end_m_temp]

    return period_start_m, period_end_m

def generate_weekly_time_periods(start_date,end_date):

    period_start = []

    # keep adding dates until exceed our end_date

    start_date = parse(start_date)
    end_date = parse(end_date)

    while start_date <= end_date:
        period_start.append(start_date)

        start_date+=dt.timedelta(7)

    # period end is -1 day from next start date + period end

    period_end = [d - dt.timedelta(1) for d in period_start[1:]]
    period_end.append(end_date)

    # turn it into string
    
    period_start_w = [d.strftime('%Y%m%d') for d in period_start]

    period_end_w = [d.strftime('%Y%m%d') for d in period_end]

    return period_start_w, period_end_w
    
def generate_3d_time_periods(start_date,end_date):

    period_start = []

    start_date = parse(start_date)
    end_date = parse(end_date)

    # keep adding dates until exceed our end_date

    while start_date <= end_date:
        period_start.append(start_date)

        start_date+=dt.timedelta(3)

    # period end is -1 day from next start date + period end

    period_end = [d - dt.timedelta(1) for d in period_start[1:]]
    period_end.append(end_date)

    # turn it into string
    
    period_start_3d = [d.strftime('%Y%m%d') for d in period_start]

    period_end_3d = [d.strftime('%Y%m%d') for d in period_end]

    return period_start_3d, period_end_3d

def generate_1d_time_periods(start_date,end_date):

    period_start = []

    start_date = parse(start_date)
    end_date = parse(end_date)

    # keep adding dates until exceed our end_date

    while start_date <= end_date:
        period_start.append(start_date)

        start_date+=dt.timedelta(1)

    # period end is -1 day from next start date + period end

    period_end = period_start
    #period_end.append(end_date)

    # turn it into string
    
    period_start_1d = [d.strftime('%Y%m%d') for d in period_start]

    period_end_1d = [d.strftime('%Y%m%d') for d in period_end]

    return period_start_1d, period_end_1d

def generate_5d_time_periods(start_date,end_date):

    period_start = []

    # keep adding dates until exceed our end_date

    start_date = parse(start_date)
    end_date = parse(end_date)

    while start_date <= end_date:
        period_start.append(start_date)

        start_date+=dt.timedelta(5)

    # period end is -1 day from next start date + period end

    period_end = [d - dt.timedelta(1) for d in period_start[1:]]
    period_end.append(end_date)

    # turn it into string
    
    period_start_5d = [d.strftime('%Y%m%d') for d in period_start]

    period_end_5d = [d.strftime('%Y%m%d') for d in period_end]

    return period_start_5d, period_end_5d
    
def generate_2w_time_periods(start_date: str,end_date: str):

    period_start = []

    # str to dt

    start_date = parse(start_date)
    end_date = parse(end_date)

    # generate period start first

    while start_date <= end_date:
        period_start.append(start_date)

        day = start_date.day
        month = start_date.month
        year = start_date.year

    # generate next day start. if day start < 15 then next start day should be 15. else 1
        if day<15:
            next_start_day = 15
            start_date = dt.datetime(year,month,next_start_day)
    
        if day>=15:
            next_start_day = 1
            next_start_month = month+1

            if next_start_month>12:
                next_start_month-=12
                year+=1

            start_date = dt.datetime(year,next_start_month,next_start_day)

    # if start day is on 1st, then end day is on 14
    # if start day is on 15, then we generate month end date
    # we take 1 less from period start and add the last end_date

    period_end = [dt.datetime(x.year,x.month,14) if x.day <15 else dt.datetime(x.year,x.month,get_month_end_date(x).day) for x in period_start[:-1]]

    period_end.append(end_date)

    # return as list of strs
    
    period_start1 = [d.strftime('%Y%m%d') for d in period_start]

    period_end1 = [d.strftime('%Y%m%d') for d in period_end]

    return period_start1, period_end1

def split_period_list(start_period_list, end_period_list, part: int):

    split_point = int(np.floor(len(start_period_list)/2))

    if part == 1:

        split_start_list = start_period_list[:split_point]
        split_end_list = end_period_list[:split_point]

        return split_start_list, split_end_list

    if part == 2:

        split_start_list = start_period_list[split_point:]
        split_end_list = end_period_list[split_point:]

        return split_start_list, split_end_list

def get_time_periods(delta: int, delta_unit: str, time_func_name='generate_monthly_time_periods', end_date = (dt.datetime.now().date()-dt.timedelta(days=1))):

    '''

    This function creates the start and end periods in the form of lists
    '''
    # default end_date is yesterday's date

    # run a custom function to get the 1st day of the historical month which we want to query from e.g. 2 mths ago

    if delta_unit == 'years':

        start_date = get_year_start(dt.datetime.now(), delta)

        period_start, period_end = eval(f'{time_func_name}("{start_date}","{end_date}")')

    if delta_unit == 'months':

        start_date = get_month_start(dt.datetime.now(), delta)

        period_start, period_end = eval(f'{time_func_name}("{start_date}","{end_date}")')

    if delta_unit == 'days':

        start_date = (dt.datetime.now()-dt.timedelta(days=delta)).date()

        period_start, period_end = eval(f'{time_func_name}("{start_date}","{end_date}")')
    
    if delta_unit =='years_whole':

        start_date = get_year_start(dt.datetime.now(), delta)

        period_start, period_end =  [start_date.strftime('%Y%m%d')], [end_date.strftime('%Y%m%d')]

    if delta_unit =='months_whole':

        start_date = get_month_start(dt.datetime.now(), delta)

        period_start, period_end = [start_date.strftime('%Y%m%d')], [end_date.strftime('%Y%m%d')]

    if delta_unit =='days_whole':

        start_date = get_day_start(dt.datetime.now(), delta)

        period_start, period_end = [start_date.strftime('%Y%m%d')], [end_date.strftime('%Y%m%d')]

    return period_start, period_end

def get_time_hh_mm_ss(sec):
    # create timedelta and convert it into string

    dt_str = str(dt.timedelta(seconds=sec))

    # split string into individual component
    
    x = dt_str.split(':')
    
    seconds = round(float(x[2]))

    str_time = f'{x[0]} Hrs, {x[1]} Mins, {seconds} Secs'

    return str_time


