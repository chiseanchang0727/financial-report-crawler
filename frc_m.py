import pandas as pd
import requests
from io import StringIO
import time
import random
import mysql.connector
from datetime import datetime
import requests


# 連接 MySQL 資料庫
db = mysql.connector.connect(host="138.3.214.21",
    user="cust-update", passwd="yfy0109!", db="CrawlerDB")
cursor = db.cursor()

def crawl_financial_Report(year,season,stock_number,url):
    
    form_data = {
        'encodeURIComponent':1,
        'step':1,
        'firstin':1,
        'off':1,
        'co_id':stock_number,
        'year': year,
        'season': season,
    }
    print(year,season,stock_number,url)

    r = requests.post(url,form_data)
    
    try:
        html_df = pd.read_html(r.text)[1].fillna("")   
        return html_df 
    except ValueError:
        print("No tables found.")

def insert_table(df,year,season,stock_number,company_name, report_name):
    seq = 1
    for index, row  in df.iterrows():  
        sql = "INSERT INTO mops_season_report (report_name, company_id, company_name,period_year,season,acct_name, this_year_amt, this_year_percent,last_year_amt, last_year_percent,creation_date,seq)  VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        if row['本季金額']=='' :  row['本季金額']=0 
        if row['本季金額(%)'] =='' :  row['本季金額(%)']=0  
        if row['去年同期金額'] =='' :  row['去年同期金額']=0  
        if row['去年同期金額(%)'] =='' :  row['去年同期金額(%)']=0 
        now = datetime.now()
        val = (report_name,stock_number, company_name,year,season, row['會計項目'] , row['本季金額'], row['本季金額(%)'], row['去年同期金額'], row['去年同期金額(%)'],now,seq)
        cursor.execute(sql, val)
        seq += 1
        db.commit()     

db = mysql.connector.connect(host="138.3.214.21",
    user="cust-update", passwd="yfy0109!", db="CrawlerDB")
cursor = db.cursor()

# 網站連線資料
BalanceSheetURL = "https://mops.twse.com.tw/mops/web/ajax_t164sb03";      # 資產負債表
ProfitAndLoseURL = "https://mops.twse.com.tw/mops/web/ajax_t164sb04";    # 損益表
CashFlowStatementURL = "https://mops.twse.com.tw/mops/web/ajax_t164sb05"; # 現金流量表
# 讀取全部公司代碼
year = 112
season = 4
comp_sql = 'SELECT distinct(company_id) , company_name FROM mops_monthly_report WHERE company_id NOT IN (SELECT DISTINCT company_id FROM  mops_season_report where period_year = %s and season = %s ) ORDER BY company_id'
sql_var = (year,season)
cursor.execute(comp_sql,sql_var)
comp_list = cursor.fetchall()       



for id in comp_list:
    stock_number = id[0]
    company_name = id[1]
    df1 = crawl_financial_Report(year,season,stock_number,BalanceSheetURL)
    if df1 is None or len(df1.axes[1]) < 6: 
        print('stocknumber:',stock_number, '  BalanceSheet is None !')
    else:    
        df1= df1.iloc[:, [0,1,2,3,4]]
        df1.columns = [('會計項目'),('本季金額'), ('本季金額(%)'), ('去年同期金額'),('去年同期金額(%)')]
        insert_table(df1,year,season,stock_number,company_name, 'BalanceSheet')
        time.sleep(random.randint(10,30))#隨機停等秒數
        df2 = crawl_financial_Report(year,season,stock_number,ProfitAndLoseURL)
        if df2 is None or len(df2.axes[1]) < 6: 
            print('stocknumber:',stock_number, '  ProfitAndLose is None !')
        else:    
            df2= df2.iloc[:, [0,1,2,3,4]]
            df2.columns = [('會計項目'),('本季金額'), ('本季金額(%)'), ('去年同期金額'),('去年同期金額(%)')]
            insert_table(df2,year,season,stock_number,company_name, 'ProfitAndLose')
            time.sleep(random.randint(10,30))#隨機停等秒數    
            df3 = crawl_financial_Report(year,season,stock_number,CashFlowStatementURL)
            if df3 is None or len(df3.axes[1]) < 6:    
                print('stocknumber:',stock_number, '  CashFlowStatement is None !')
            else:                
                df3= df3.iloc[:, [0,1,2,3,4]]
                df3.columns = [('會計項目'),('本季金額'), ('去年同期金額'), ('本季金額(%)'), ('去年同期金額(%)')]
                insert_table(df3,year,season,stock_number,company_name, 'CashFlowStatement')
                print('ok')
    time.sleep(random.randint(10,30))#隨機停等秒數