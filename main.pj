import requests
import pandas as pd
import geopandas as gpd
from bs4 import BeautifulSoup
import mysql.connector
from datetime import datetime
from mysql.connector import errorcode

def call_api(date, market_name):
    api_key = "10b9d3901e6371fb1f469eb676196b83c27fc5087dbe7e4f15be1ff307398691"
    base_url = "http://211.237.50.150:7080/openapi/"
    api_url = "Grid_20151127000000000313_1"
    url = (
        f"{base_url}{api_key}/xml/{api_url}/1/1000?DELNG_DE={date}&WHSAL_MRKT_NM={market_name}도매시장"
    )
    response = requests.get(url)
    xml_data = response.text
    soup = BeautifulSoup(xml_data, 'xml')

    return soup

# 전체 데이터 개수 추출
def extract_total_count(soup):
    total_count = int(soup.find("totalCnt").text)
    return total_count


def create_dataframe(soup,data_list):
    for item in soup.find_all("row"):
        data = {
            "ROW_NUM": item.find("ROW_NUM").text,
            "DELNG_DE": item.find("DELNG_DE").text,
            "SBID_TIME": item.find("SBID_TIME").text,
            "AUC_SE_CODE": item.find("AUC_SE_CODE").text,
            "AUC_SE_NM": item.find("AUC_SE_NM").text,
            "WHSAL_MRKT_NEW_CODE": item.find("WHSAL_MRKT_NEW_CODE").text,
            "WHSAL_MRKT_NEW_NM": item.find("WHSAL_MRKT_NEW_NM").text,
            "WHSAL_MRKT_CODE": item.find("WHSAL_MRKT_CODE").text,
            "WHSAL_MRKT_NM": item.find("WHSAL_MRKT_NM").text,
            "CPR_INSTT_NEW_CODE": item.find("CPR_INSTT_NEW_CODE").text,
            "INSTT_NEW_NM": item.find("INSTT_NEW_NM").text,
            "CPR_INSTT_CODE": item.find("CPR_INSTT_CODE").text,
            "INSTT_NM": item.find("INSTT_NM").text,
            "LEDG_NO": item.find("LEDG_NO").text,
            "SLE_SEQN": item.find("SLE_SEQN").text,
            "CATGORY_NEW_CODE": item.find("CATGORY_NEW_CODE").text,
            "CATGORY_NEW_NM": item.find("CATGORY_NEW_NM").text,
            "CATGORY_CODE": item.find("CATGORY_CODE").text,
            "CATGORY_NM": item.find("CATGORY_NM").text,
            "STD_PRDLST_NEW_CODE": item.find("STD_PRDLST_NEW_CODE").text,
            "STD_PRDLST_NEW_NM": item.find("STD_PRDLST_NEW_NM").text,
            "STD_PRDLST_CODE": item.find("STD_PRDLST_CODE").text,
            "STD_PRDLST_NM": item.find("STD_PRDLST_NM").text,
            "STD_SPCIES_NEW_CODE": item.find("STD_SPCIES_NEW_CODE").text,
            "STD_SPCIES_NEW_NM": item.find("STD_SPCIES_NEW_NM").text,
            "STD_SPCIES_CODE": item.find("STD_SPCIES_CODE").text,
            "STD_SPCIES_NM": item.find("STD_SPCIES_NM").text,
            "STD_UNIT_NEW_CODE": item.find("STD_UNIT_NEW_CODE").text,
            "STD_UNIT_NEW_NM": item.find("STD_UNIT_NEW_NM").text,
            "STD_FRMLC_NEW_CODE": item.find("STD_FRMLC_NEW_CODE").text,
            "STD_FRMLC_NEW_NM": item.find("STD_FRMLC_NEW_NM").text,
            "STD_MG_NEW_CODE": item.find("STD_MG_NEW_CODE").text,
            "STD_MG_NEW_NM": item.find("STD_MG_NEW_NM").text,
            "STD_QLITY_NEW_CODE": item.find("STD_QLITY_NEW_CODE").text,
            "STD_QLITY_NEW_NM": item.find("STD_QLITY_NEW_NM").text,
            "CPR_USE_PRDLST_CODE": item.find("CPR_USE_PRDLST_CODE").text,
            "CPR_USE_PRDLST_NM": item.find("CPR_USE_PRDLST_NM").text,
            "SBID_PRIC": item.find("SBID_PRIC").text,
            "SHIPMNT_SE_CODE": item.find("SHIPMNT_SE_CODE").text,
            "SHIPMNT_SE_NM": item.find("SHIPMNT_SE_NM").text,
            "STD_MTC_NEW_CODE": item.find("STD_MTC_NEW_CODE").text,
            "STD_MTC_NEW_NM": item.find("STD_MTC_NEW_NM").text,
            "CPR_MTC_CODE": item.find("CPR_MTC_CODE").text,
            "CPR_MTC_NM": item.find("CPR_MTC_NM").text,
            "DELNG_QY": item.find("DELNG_QY").text
        }
        data_list.append(data)
    df = pd.DataFrame(data_list)
    return df



def update(where):
    data_list = []
    today = datetime.now().strftime("%Y%m%d")
    soup = call_api(today,where)
    df = create_dataframe(soup, data_list)
    for index, row in df.iterrows():
        # Markets
        cursor.execute(
            "INSERT IGNORE INTO Markets (MarketID, Name) VALUES (%s, %s)",
            (row['WHSAL_MRKT_CODE'], row['WHSAL_MRKT_NM'])
        )

        # Products
        cursor.execute(
            "INSERT IGNORE INTO Products (Productid,Name, Category) VALUES (%s,%s, %s)",
            (row['STD_PRDLST_CODE'], row['STD_PRDLST_NM'], row['CATGORY_NM'])
        )

        # AuctionPrices
        cursor.execute(
            "INSERT IGNORE INTO AuctionPrices (MarketID, ProductID, Date, Price, Unit, Unitname, Grade, Origin) VALUES ( %s,%s, %s, %s, %s, %s, %s, %s)",
            (row['WHSAL_MRKT_CODE'], row['STD_PRDLST_CODE'], row['DELNG_DE'], row['SBID_PRIC'], row['DELNG_QY'],
             row['STD_UNIT_NEW_NM'], row['STD_QLITY_NEW_NM'], row['STD_MTC_NEW_NM'])
        )
    conn.commit()
    print("업데이트 완료!")



try:
    conn = mysql.connector.connect(
        host="127.0.0.1",  # MySQL 호스트 이름
        user="root",  # MySQL 사용자 이름
        password="xodn0812",  # MySQL 비밀번호
        database="market_data"  # 사용할 데이터베이스 이름
    )
    print("Connected to MySQL Database")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Error: Access denied. Check your username and password.")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Error: The specified database does not exist.")
    else:
        print(f"Error: {err}")
    exit()
# 사용자 입력 받기
print("시장과 원하는 품목을 입력하세요! x를 입력하면 종료합니다. 업데이트 하려면 update를 입력해 주세요")
while 1:
    cursor = conn.cursor()
    y = input("Enter the market name: ")
    if y=="x":
        cursor.close()
        conn.close()
        break
    x = input("Enter the product name: ")
    if x=="x":
        cursor.close()
        conn.close()
        break
    if x=="update":
        update(y)
    else:
        query = """
                SELECT  AuctionPrices.Date,Price,Grade,Origin
                FROM Products, AuctionPrices,Markets
                WHERE Products.productID = AuctionPrices.productID
                AND AuctionPrices.marketID = Markets.marketID
                AND Products.name = %s
                LIMIT 10
            """
        cursor.execute(query, (x,),(y,))

        results = cursor.fetchall()
        if results == []:
            print("일치하는 품목이 없습니다.")
        for result in results:

            formatted_result = "날짜:{},가격:{},등급:'{}',원산지:'{}'".format(
                result[0].strftime("%Y%m%d"),
                int(result[1]),
                result[2],
                result[3]
            )
            print(formatted_result)

cursor.close()
conn.close()

