# Import required libraries
from bs4 import BeautifulSoup, SoupStrainer
from multiprocessing import Process
from googletrans import Translator
import concurrent.futures
import requests
import sqlite3
import random
import psutil
import time
import os

import pandas as pd
import regex as re
import numpy as np
pd.set_option("display.max_rows", None, "display.max_columns", None,'display.max_colwidth',1)


#Scraper function
def soup(url,strainer):
    # Set the User-Agent to mimic a web browser
    
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
    ]

    # Use a session object to maintain a persistent connection
    session = requests.Session()

    headers = {'User-Agent': random.choice(user_agents)}

    session.headers.update(headers)
    

    # Make a GET request and parse the HTML with Beautiful Soup
    r = session.get(url, headers=headers,timeout = 20)

    #Extract specific part of the website
    strain = SoupStrainer(strainer)

    # Extract the data you need from the HTML
    s=BeautifulSoup(r.content, 'lxml', parse_only=strain)

    # Add a random delay between 1 to 3 seconds between requests to avoid detection    
    time.sleep(random.randint(1, 5))

    return s      


urls = ['https://www.subito.it/annunci-italia/vendita/immobili/',
        'https://www.subito.it/annunci-italia/cerco/immobili/',
        'https://www.subito.it/annunci-italia/affitto/immobili/',
        'https://www.subito.it/annunci-italia/affitto-vacanze/immobili/']


def futures_transformer(s, url_in, category, count, workers=len(urls)):
    with concurrent.futures.ThreadPoolExecutor(max_workers = workers) as executor:
        args = [(u, category, count) for u in url_in]
        future = executor.map(lambda p: s(*p), args,timeout=100)

def futures_scraper(s, url_in,workers=len(urls)):
    with concurrent.futures.ThreadPoolExecutor(max_workers = workers) as executor:
        future = [executor.map(s, url_in,timeout=100)]
        

def translate(col):
    t = Translator()
    t_col = col.apply(lambda x: t.translate(x, dest='en', timeout=100).text)
    #print(t_col)
    return t_col



def transformer(url_in, category, count, db="scraped_db.db" ,csv="subito_scraped.csv"):
    conn=sqlite3.connect(db)
    query = "SELECT * FROM subito"
    df_sqlite = pd.read_sql(query, conn)
    
    while url_in not in df_sqlite['URL'].values:
        
        if category==urls[0]:
            cat_txt = 'Sale'
    
        elif category==urls[1]:
            cat_txt = 'Wanted'
    
        elif category==urls[2]:
            cat_txt = 'Rent'
    
        elif category==urls[3]:
            cat_txt = 'Rent for Holidays'
    
        else:
            print("CATEGORY DOESN'T EXIST")
    
        print(f"[{category}?o={count}] [{url_in}]\t CATEGORY: {cat_txt}")
    
        id_strainer = "div",{"general-info_ad-info___SSdI"}
        id_span = soup(url_in, id_strainer).find("span",{"class":"index-module_sbt-text-atom__ed5J9 index-module_token-caption__TaQWv size-normal index-module_weight-book__WdOfA AdInfo_ad-info__id__g3sz1"})
        id_txt = id_span.text.strip()
        id_txt = re.findall(r"\d+", id_txt)
        id_txt = id_txt[0]
        print(f"[{category}?o={count}] [{url_in}]\t ID: {id_txt}")
    
        prop_strainer = "div",{"general-info_ad-info___SSdI"}
        prop_h1 = soup(url_in, prop_strainer).find("h1",{"class":"index-module_sbt-text-atom__ed5J9 index-module_token-h4__x776H size-normal index-module_weight-semibold__MWtJJ AdInfo_ad-info__title__7jXnY"})
        prop_txt = prop_h1.text.strip()
        print(f"[{category}?o={count}] [{url_in}]\t PROPERTY: {prop_txt}")
    
        seller_strainer = "div",{"class":"sellerInfo__content advertiser-info-section_sellerInfo__content__orn7P"}
        seller_h6 = soup(url_in, seller_strainer).find("h6",{"class":"index-module_sbt-text-atom__ed5J9 index-module_token-h6__FGmXw size-normal index-module_weight-semibold__MWtJJ"})
        if seller_h6:
            seller_txt = seller_h6.text.strip()
            print(f"[{category}?o={count}] [{url_in}]\t SELLER-NAME: {seller_txt}")
        else:
            seller_h6 = soup(url_in, seller_strainer).find("h6",{"class":"index-module_sbt-text-atom__ed5J9 index-module_token-h6__FGmXw size-normal index-module_weight-semibold__MWtJJ index-module_name__hRS5a"})
            seller_txt = seller_h6.text.strip()
            print(f"[{category}?o={count}] [{url_in}]\t SELLER-NAME: {seller_txt}")
    
        web_strainer = "div",{"class":"sellerInfo__content advertiser-info-section_sellerInfo__content__orn7P"}
        web_a = soup(url_in, web_strainer).find("a",{"class":"index-module_website_link__8U62f"})
        if web_a:
            web_txt = web_a.get('href')
            print(f"[{category}?o={count}] [{url_in}]\t SELLER-WEBSITE: {web_txt}")
        elif soup(url_in,web_strainer).find("a",{"class":"index-module_anchor_button__3j4qS"}):
            web_a = soup(url_in,web_strainer).find("a",{"class":"index-module_anchor_button__3j4qS"})
            web_txt = web_a.get('href')
            print(f"[{category}?o={count}] [{url_in}]\t SELLER-WEBSITE: {web_txt}")    
        elif soup(url_in, web_strainer).find("a",{"class":"index-module_rounded_user_badge__KC3zi"}):
            web_a = soup(url_in, web_strainer).find("a",{"class":"index-module_rounded_user_badge__KC3zi"})
            web_txt = f"https://www.subito.it{web_a.get('href')}"
            print(f"[{category}?o={count}] [{url_in}]\t SELLER-WEBSITE: {web_txt}")
        else:
            web_txt = ''
            print(f"[{category}?o={count}] [{url_in}]\t SELLER-WEBSITE: {web_txt}")
    
        ad_strainer = "div",{"class":"sellerInfo__content advertiser-info-section_sellerInfo__content__orn7P"}
        ad_p = soup(url_in, ad_strainer).find("p",{"class":"index-module_sbt-text-atom__ed5J9 index-module_token-body__GXE1P size-normal index-module_weight-book__WdOfA index-module_body_text__v5hiP"})
        if ad_p:
            ad_txt = ad_p.text.strip()
            print(f"[{category}?o={count}] [{url_in}]\t SELLER-ADDRESS: {ad_txt}")
        else:
            ad_txt = ''
            print(f"[{category}?o={count}] [{url_in}]\t SELLER-ADDRESS: {ad_txt}")
    
        img_strainer = "section", {"class":"grid_detail-component__7sBtj grid_gallery__vV2Mf"}
        img_nav = soup(url_in, img_strainer).find("nav",{"class":"Thumbnails_thumbnail-wrapper__mua8L"})
    
        if img_nav:
            img_txt = ', '.join([i.find('img').get('src') for i in img_nav])
            print(f"[{category}?o={count}] [{url_in}]\t IMAGES: {img_txt}")
        elif soup(url_in, img_strainer).find("figure",{"class":"flickity-viewport Carousel_carousel-cell__bHxmt"}):
            img_nav = soup(url_in, img_strainer).find("figure",{"class":"flickity-viewport Carousel_carousel-cell__bHxmt"})
            img_txt = img_nav.find('img').get('src')
            print(f"[{category}?o={count}] [{url_in}]\t IMAGES: {img_txt}")
        else:
            img_txt = ''
            print(f"[{category}?o={count}] [{url_in}]\t IMAGES: {img_txt}")
    
        nav_strainer = "span",{"style_navigation__breadcrumbs__kbiU3"}
        nav_span = soup(url_in, nav_strainer).find_all("span",{"itemprop":"name"})
        type_txt = [i.text.strip() for i in nav_span][1]
        print(f"[{category}?o={count}] [{url_in}]\t PROPERTY TYPE: {type_txt}")
    
        reg_txt = [i.text.strip() for i in nav_span][2]
        print(f"[{category}?o={count}] [{url_in}]\t REGION: {reg_txt}")
    
        prov_txt = [i.text.strip() for i in nav_span][3]
        prov_txt = prov_txt.replace(" (Prov)","")
        print(f"[{category}?o={count}] [{url_in}]\t PROVINCE: {prov_txt}")
    
        loc_txt = [i.text.strip() for i in nav_span][4]
        print(f"[{category}?o={count}] [{url_in}]\t LOCATION: {loc_txt}")
    
        price_strainer = "div",{"general-info_ad-info___SSdI"}
        price_p = soup(url_in, price_strainer).find("p",{"class":"index-module_price__N7M2x AdInfo_ad-info__price__tGg9h index-module_large__SUacX"})
        try:
            price_txt = price_p.text.strip()
            price_txt = price_txt.replace(".",",")
            price_txt = re.findall(r"\d+\W*\d+", price_txt)[0]
            print(f"[{category}?o={count}] [{url_in}]\t PRICE: {price_txt}")
        except:
            price_txt = ''
            print(f"[{category}?o={count}] [{url_in}]\t PRICE: {price_txt}")
    
        desc_strainer = "section",{"class":"grid_detail-component__7sBtj grid_description__rEv3i"}
        desc_p = soup(url_in, desc_strainer).find("p",{"class":"index-module_sbt-text-atom__ed5J9 index-module_token-body__GXE1P size-normal index-module_weight-book__WdOfA AdDescription_description__gUbvH index-module_preserve-new-lines__ZOcGy"})
        desc_txt = desc_p.text.strip()
        print(f"[{category}?o={count}] [{url_in}]\t DESCRIPTION: {desc_txt}")
    
        det_strainer = "div",{"class":"feature-list-section_detail-chip-container__by96k"}
        det_p = soup(url_in,det_strainer).find_all("p",{"class":"index-module_sbt-text-atom__ed5J9 index-module_token-caption__TaQWv size-normal index-module_weight-book__WdOfA StaticChip-module_static-chip__va4RV StaticChip-module_medium__OZRaA"})
        if det_p:
            det_txt = ', '.join([i.text.strip() for i in det_p])
            print(f"[{category}?o={count}] [{url_in}]\t DETAILS: {det_txt}")
        else:
            det_txt = ''
            print(f"[{category}?o={count}] [{url_in}]\t DETAILS: {det_txt}")
    
        others_strainer = "section",{"class":"grid_detail-component__7sBtj grid_detail-component__7sBtj"}
        others_key = soup(url_in, others_strainer).find_all("span",{"class":"index-module_sbt-text-atom__ed5J9 index-module_token-body__GXE1P size-normal index-module_weight-book__WdOfA feature-list_label__Jf58a"})
        others_val = soup(url_in, others_strainer).find_all("span",{"class":"index-module_sbt-text-atom__ed5J9 index-module_token-body__GXE1P size-normal index-module_weight-book__WdOfA feature-list_value__pgiul"})
        keys = [i.text.strip().upper() for i in others_key]
        values = [i.text.strip() for i in others_val]
        key_val = {a:b for a,b in zip(keys, values)}
        key_val2 = [kv_merge.remove(d) for d in key_val if not d]
        print(f"[{category}?o={count}] [{url_in}]\t OTHERS: {key_val}")
    
        scraped_data = {"ID":id_txt, 
                        "PROPERTY":prop_txt,
                        "DESCRIPTION":desc_txt,
                        "IMAGES":img_txt,
                        "CATEGORY":cat_txt,
                        "PROPERTY TYPE":type_txt,
                        'REGION':reg_txt, 
                        "PROVINCE":prov_txt, 
                        "LOCATION":loc_txt, 
                        "PRICE (EURO)":price_txt, 
                        "DETAILS":det_txt,
                        "URL":url_in,
                        "SELLER-NAME":seller_txt,
                        "SELLER-ADDRESS":ad_txt,
                        "SELLER-WEBSITE":web_txt}
    
    
        df = pd.DataFrame([scraped_data])
    
        new_df = pd.DataFrame([key_val])
    
        t = Translator()
        new_df.columns = [t.translate(col, src='it', dest='en', timeout=10).text for col in new_df.columns]
    
        df_merge = df.join(new_df, how='inner')
    
        with concurrent.futures.ThreadPoolExecutor() as executor:
            translated_cols = list(executor.map(translate, [df_merge[col] for col in df_merge.columns]))
        for i, col in enumerate(df_merge.columns):
            df_merge[col] = translated_cols[i]
    
        df_merge = df_merge.replace({"SURFACE":"\s\w+\d*"}, {"SURFACE": ""},regex=True)
        df_merge = df_merge.rename(columns={'SURFACE': 'SURFACE (m2)'})
        df_merge = df_merge.rename(columns={'LOCALS': 'ROOMS'})
        df_merge = df_merge.replace(regex=r"^-$", value=np.nan)
        df_merge = df_merge.replace('',np.nan)
    
        try: 
            df_merge['ENERGY RATING'] = df_merge['ENERGY RATING'].str.upper()
        except:
            pass
        

        df_sqlite = pd.read_sql(query, conn)
        concatenated_df = pd.concat([df_sqlite, df_merge], ignore_index=True, sort=False)
        concatenated_df = concatenated_df.dropna(how='all')
        concatenated_df = concatenated_df.drop_duplicates(subset=['ID'],ignore_index=True)
        print(f"[{category}?o={count}] : CONCATENATED")
        
        df_sqlite = pd.read_sql(query, conn)
        if concatenated_df.shape[1] > df_sqlite.shape[1]:
            concatenated_df.to_sql('subito', conn, if_exists='replace', index=False)
            sqlite_saved = pd.read_sql(query, conn)
            conn.close()
            print(f"[{category}?o={count}] REPLACED subito table\t DATABASE SIZE: [{len(sqlite_saved)}]")
            
            concatenated_df.to_csv(csv,index=False)
            csv_saved = pd.read_csv(csv, on_bad_lines='warn')
            print(f"[{category}?o={count}] OVERWRITTEN {csv}\t CSV SIZE: [{len(csv_saved)}]")
            
            
        else:
            concatenated_df.iloc[-1:].to_sql('subito', conn, if_exists='append', index=False)
            sqlite_saved = pd.read_sql(query, conn)
            conn.close()
            print(f"[{category}?o={count}] APPENDED subito table\t DATABASE SIZE: [{len(sqlite_saved)}]")
            
            concatenated_df.iloc[-1:].to_csv(csv,index=False, mode='a', header=False)       
            csv_saved = pd.read_csv(csv, on_bad_lines='warn')
            print(f"[{category}?o={count}] APPENDED {csv}\t CSV SIZE: [{len(csv_saved)}]")  
            
        
    return


def check_size(db="scraped_db.db",csv="subito_scraped.csv"):
    conn=sqlite3.connect(db)
    query = "SELECT * FROM subito"
    
    df_sqlite = pd.read_sql(query, conn)
    df_csv = pd.read_csv(csv, on_bad_lines='warn')
    if len(df_sqlite) < len(df_csv):
        df_csv.to_sql('subito', conn, if_exists='replace', index=False)
        sqlite_saved = pd.read_sql(query,conn)
        print(f"'{db}' IS UPDATED\t DATABASE SIZE: [{len(sqlite_saved)}]")

    elif len(df_sqlite) > len(df_csv):
        df_sqlite.to_csv(csv,index=False)
        csv_saved = pd.read_csv(csv, on_bad_lines='warn')
        print(f"'{csv}' IS UPDATED\t CSV SIZE: [{len(csv_saved)}]")

    else:
        csv_saved = pd.read_csv(csv, on_bad_lines='warn')
        sqlite_saved = pd.read_sql(query,conn)
        print(f"'{csv}' and '{db}' HAS THE SAME SIZE\n CSV SIZE: [{len(csv_saved)}]\n DATABASE SIZE: [{len(sqlite_saved)}]")
    
    return


def scraper(url_in):
    count = 0
    while True:
        count += 1
        strainer = "div",{"class":"items__item item-card item-card--big BigCard-module_card__Exzqv"}
        a = soup(url_in+f"?o={count}", strainer).find_all("a",{"class":"BigCard-module_link__kVqPE"})
        if a:
            url = [links.get('href') for links in a]
            print(f"[{count}]\t SCRAPING: [{url_in}?o={count}]")
            futures_transformer(transformer, url, url_in, count, workers=8)
            print(f"[{count}]\t ALREADY SCRAPED: [{url_in}?o={count}]")
                
        else:
            print(f"{url_in}?o={count} DOES NOT EXIST!")    
            break

    return
    

if __name__ == '__main__':
    check_size()
    futures_scraper(scraper,urls)


