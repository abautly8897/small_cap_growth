# -*- coding: utf-8 -*-

# @Time : 2023/4/6 14:01

# @Author : Chris Wang

# @FileName: security_fundamental_data.py

# @Project:$ {PROJECT_NAME}


import datetime
import sqlite3

import pandas as pd


# 通过本地
def get_local_db_stock_fin_abstract(period=0, code=0, period_range=0, target_date=0, indicator=''):
    '''

    :param period:
    :param code:
    :param period_range:
    :param target_date:
    :param indicator:
    ['ann_date', 'period', 'code', '归母净利润', '营业总收入', '营业成本', '净利润', '扣非净利润', '股东权益合计(净资产)', '商誉',
     '经营现金流量净额', '基本每股收益', '每股净资产', '每股现金流', '净资产收益率(ROE)', '总资产报酬率(ROA)', '毛利率', '销售净利率', '
     期间费用率', '资产负债率', '稀释每股收益', '摊薄每股净资产_期末股数', '调整每股净资产_期末股数', '每股净资产_最新股数',
     '每股经营现金流', '每股现金流量净额', '每股企业自由现金流量', '每股股东自由现金流量', '每股未分配利润', '每股资本公积金',
     '每股盈余公积金', '每股留存收益', '每股营业收入', '每股营业总收入', '每股息税前利润', '摊薄净资产收益率', '净资产收益率_平均',
      '净资产收益率_平均_扣除非经常损益', '摊薄净资产收益率_扣除非经常损益', '息税前利润率', '总资产报酬率', '总资本回报率', '投入资本回报率',
       '息前税后总资产报酬率_平均', '成本费用利润率', '营业利润率', '总资产净利率_平均', '总资产净利率_平均(含少数股东损益)', '营业总收入增长率',
        '归属母公司净利润增长率', '经营活动净现金/销售收入', '经营性现金净流量/营业总收入', '成本费用率', '销售成本率', '经营活动净现金/归属母公司的净利润',
        '所得税/利润总额', '流动比率', '速动比率', '保守速动比率', '权益乘数', '权益乘数(含少数股权的净资产)', '产权比率', '现金比率', '应收账款周转率',
        '应收账款周转天数', '存货周转率', '存货周转天数', '总资产周转率', '总资产周转天数', '流动资产周转率', '流动资产周转天数', '应付账款周转率']
    :return:
    example1:
    period_range = ['20200331','20220331']
    indicator = '每股营业收入'
    get_stock_fin_abstract(period_range=period_range, indicator=indicator)
    example2:
    code = '000004'
    period = '20220331'
    indicator = '每股营业收入'
    get_stock_fin_abstract(period=period, indicator=indicator)
    '''
    # 连接到SQLite数据库
    conn = sqlite3.connect('file:J:\Quant\DataBase_sqlite\example.db?mode=ro', uri=True)

    if indicator == '':
        output_cols_str = '*'
    else:
        selected_cols = ['code', 'ann_date', 'period']
        if type(indicator) is str:
            selected_cols.append(indicator)
        else:
            selected_cols.extend(indicator)
        output_cols_str = ','.join(selected_cols)
    period_range = ['20200331', '20220331']
    target_date = 0
    # 查询表中符合条件的数据并将结果转换为DataFrame对象
    table_name = 'stock_financial_abstract'
    if type(period) is str and type(code) is str:
        query = f"SELECT {output_cols_str} FROM {table_name} WHERE code='{code}' AND period={period}"
    elif type(period) is str and type(code) is list:
        query = f"SELECT {output_cols_str} FROM {table_name} WHERE  period = '{period}' AND code IN ({','.join(map(str, code))})"
    elif type(period_range) is list and type(code) is list:
        query = f"SELECT {output_cols_str} FROM {table_name} WHERE  period BETWEEN '{period_range[0]}' AND '{period_range[1]}' AND code IN ({','.join(map(str, code))})"
    elif period == 0 and type(code) is list:
        query = f"SELECT {output_cols_str} FROM {table_name} WHERE  period BETWEEN '{period_range[0]}' AND '{period_range[1]}' AND code IN ({','.join(map(str, code))})"
    elif type(period) is str and code == 0:
        query = f"SELECT {output_cols_str} FROM {table_name} WHERE period={period}"
    elif type(period_range) is list and code == 0:
        query = f"SELECT {output_cols_str} FROM {table_name} WHERE  period BETWEEN '{period_range[0]}' AND '{period_range[1]}'"
    df = pd.read_sql_query(query, conn)
    if target_date != 0:
        df = df.loc[df['ann_date'] <= target_date]
    # 关闭数据库连接
    conn.close()
    return df


# 根据提取数据的日期生成此时已经发布截至的业绩报告期
def get_yj_period(date):
    year = date[:4]
    short_date = date[4:]
    if short_date <= '0430':
        yj_period = str(int(year) - 1) + '0930'
    elif short_date <= '0830':
        yj_period = year + '0331'
    elif short_date <= '1030':
        yj_period = year + '0630'
    else:
        yj_period = year + '0930'
    return yj_period


# 根据目标period生成period_list
def get_TTM_period_list(target_period):
    if target_period[4:] != '1231':
        period_list = [target_period, str(int(target_period[:4]) - 1) + '1231',
                       str(int(target_period[:4]) - 1) + target_period[-4:]]
    else:
        period_list = [target_period]
    return period_list


# 获取tusahre财务指标数据，可指定数据品种、证券范围以及数据获取时间
def get_fin_indicators(period, indicators, security=[], date=datetime.datetime.today().strftime('%Y%m%d'),
                       market_sign='A'):
    '''
    :param security: security list
    :param date: yyyymmdd
    :param period: yyyymmdd
    :param indicators: list
    :return: dataframe
    '''
    fields = ['ts_code', 'ann_date', 'end_date', 'update_flag']
    fields.extend(indicators)
    revenue_ps_df = pro.fina_indicator_vip(period=period, fields=fields)
    revenue_ps_df.dropna(inplace=True)
    # 查看是否指定了证券区间
    if len(security) > 0:
        revenue_ps_df = revenue_ps_df.loc[revenue_ps_df['ts_code'].isin(security)]
    if market_sign == 'A':
        revenue_ps_df = revenue_ps_df[revenue_ps_df['ts_code'].str.startswith(('0', '3', '6'))]
    revenue_ps_df = revenue_ps_df[revenue_ps_df['ann_date'] <= date]
    revenue_ps_df = revenue_ps_df.reset_index()
    revenue_ps_df.drop('index', axis=1)
    return revenue_ps_df


# get_fin_indicators(date='20230316', period='20221231', indicators=['eps', 'revenue_ps'])


# 获取财务指标的TTM数据
def get_TTM_fin_data(target_period, threshold_date, indicator):
    indicators = [indicator]
    period_list = get_TTM_period_list(target_period)
    df = get_fin_indicators(target_period, indicators)
    selected_sec_ls = df.ts_code.tolist()
    if len(period_list) > 1:
        for period in period_list[1:]:
            new_df = get_fin_indicators(period, indicators)
            selected_sec_ls = list(set(selected_sec_ls) & set(new_df.ts_code.tolist()))
            df = pd.concat([df, new_df], ignore_index=True)
    df = df.loc[df['ts_code'].isin(selected_sec_ls)]
    df = df.reset_index()
    # 针对公告修正进行调整
    # 使用 Pandas 库的 groupby() 函数将 Dataframe 中具有相同 col1 和 col2 值的行分组
    groups = df.groupby(['ts_code', 'end_date'], group_keys=True)

    def filter_group(group):
        if (group['update_flag'] == '0').all():
            return group
        return group[group['update_flag'] != '0']

    df_filtered = groups.apply(filter_group)
    df_filtered = df_filtered.set_index('index')
    df_filtered = df_filtered.loc[df_filtered['ann_date'] <= threshold_date].copy()
    origin_sec_ls = list(set(df_filtered.ts_code.tolist()))
    # 得到指定指标的TTM数据
    indicator = 'revenue_ps'
    result_df = pd.DataFrame(columns=['code', 'ann_date', 'period', indicator + '_TTM'])
    i = 0
    for sec in origin_sec_ls:
        print(i, end='\r')
        ann_date = \
            df_filtered.loc[
                (df_filtered['ts_code'] == sec) & (df_filtered['end_date'] == target_period), 'ann_date'].iloc[0]
        if len(period_list) > 1:
            indicator_data = \
                df_filtered.loc[
                    (df_filtered['ts_code'] == sec) & (df_filtered['end_date'] == period_list[0]), indicator].iloc[
                    0] + \
                df_filtered.loc[
                    (df_filtered['ts_code'] == sec) & (df_filtered['end_date'] == period_list[1]), indicator].iloc[
                    0] - \
                df_filtered.loc[
                    (df_filtered['ts_code'] == sec) & (df_filtered['end_date'] == period_list[2]), indicator].iloc[
                    0]
        else:
            indicator_data = \
                df_filtered.loc[
                    (df_filtered['ts_code'] == sec) & (df_filtered['end_date'] == period_list[0]), indicator].iloc[
                    0]
        result_df.loc[i] = [sec, ann_date, target_period, indicator_data]
        i += 1
    return result_df


# 从本地数据库获取财务指标数据
def get_TTM_fin_indicator_data_from_local(indicator='每股营业收入', target_period='20220331',
                                          threshold_date='20220520'):
    period_list = get_TTM_period_list(target_period)
    df = get_local_db_stock_fin_abstract(period=target_period, indicator=indicator)
    df = df.loc[df['ann_date'] <= threshold_date].copy()
    selected_sec_ls = df.code.tolist()
    if len(period_list) > 1:
        for period in period_list[1:]:
            new_df = get_local_db_stock_fin_abstract(period=period, indicator=indicator)
            new_df = new_df.loc[new_df['ann_date'] <= threshold_date].copy()
            selected_sec_ls = list(set(selected_sec_ls) & set(new_df.code.tolist()))
            df = pd.concat([df, new_df], ignore_index=True)
    df_filtered = df.loc[df['code'].isin(selected_sec_ls)]  # 确保数据完整获取
    # 得到指定指标的TTM数据
    result_df = pd.DataFrame(columns=['code', 'ann_date', 'period', indicator + '_TTM'])
    i = 0
    for sec in selected_sec_ls:
        # print(i, end='\r')
        ann_date = \
            df_filtered.loc[
                (df_filtered['code'] == sec) & (df_filtered['period'] == target_period), 'ann_date'].iloc[0]
        if len(period_list) > 1:
            indicator_data = \
                df_filtered.loc[
                    (df_filtered['code'] == sec) & (df_filtered['period'] == period_list[0]), indicator].iloc[
                    0] + \
                df_filtered.loc[
                    (df_filtered['code'] == sec) & (df_filtered['period'] == period_list[1]), indicator].iloc[
                    0] - \
                df_filtered.loc[
                    (df_filtered['code'] == sec) & (df_filtered['period'] == period_list[2]), indicator].iloc[
                    0]
        else:
            indicator_data = \
                df_filtered.loc[
                    (df_filtered['code'] == sec) & (df_filtered['period'] == period_list[0]), indicator].iloc[
                    0]
        result_df.loc[i] = [sec, ann_date, target_period, indicator_data]
        i += 1
    return result_df


# 若该因子涉及多年数据，生成相关报告期的列表
def get_n_year_factor_period_ls(target_period, n=5):
    period_ls = []
    for i in range(n):
        new_period = str(int(target_period[:4]) - i) + target_period[-4:]
        period_ls.append(new_period)
    return period_ls
