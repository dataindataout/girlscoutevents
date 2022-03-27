#!/usr/bin/env python
# coding: utf-8

# import libraries

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from bs4 import BeautifulSoup
import pandas as pd

from urllib.parse import urlparse

import hashlib

from datetime import datetime
import time

from pangres import upsert

from sqlalchemy import create_engine

# retrieve and parse html

options = Options()
# options.headless = True
options.add_argument("--window-size=1920,1200")

# webdriver for Chrome
DRIVER_PATH = "/Applications/chromedriver"
driver = webdriver.Chrome(options=options, executable_path=DRIVER_PATH)

# add urls to list here as needed; some of our favorite councils for virtual events
urls = [
    "https://www.nccoastalpines.org/en/activities/activity-list.advanced.html",
    "https://www.gsle.org/en/events/event-list.advanced.html",
    "https://www.girlscoutstoday.org/en/events/event-list.advanced.html",
    "https://www.citrus-gs.org/en/events/event-list.advanced.html",
]

# initiate list buckets
links = []
titles = []
startdates = []
enddates = []
councils = []
uniquekeys = []

# loop through urls, process form, and get data from page
for url in urls:

    try:
        # click and submit
        driver.get(url)
        driver.find_element_by_xpath(
            "//input[contains(translate(@id, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'program-level/ambassador')]"
        ).click()
        driver.find_element_by_id("sub").submit()

        # here is the page, make the soup
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "lxml")

        # get council identifier from url
        council = urlparse(url).netloc.split(".")[1]
        primarylink = urlparse(url).netloc

        # now get the data on each page
        for x in soup.find_all(class_="eventsList eventSection"):

            link = ""
            title = ""
            startdate = ""
            enddate = ""

            link = primarylink + x.h6.a["href"]
            links.append(link)

            title = x.h6.a.text
            titles.append(title)

            startdate = x.find_next("span", itemprop="startDate")["content"]
            startdates.append(startdate)

            enddate = x.find_next("span", itemprop="stopDate")["content"]
            enddates.append(enddate)

            councils.append(council)

            uniquekey = hashlib.sha224(
                str(link + startdate).encode("utf-8")
            ).hexdigest()
            uniquekeys.append(uniquekey)

        time.sleep(5)

    except Exception as e:
        print("An error has occurred")
        print(e)
        continue

driver.quit()

# put data in a dataframe
# a df not strictly needed here, except pangres uses it

df = pd.DataFrame(
    {
        "uniquekey": uniquekeys,
        "link": links,
        "title": titles,
        "startdate": startdates,
        "enddate": enddates,
        "council": councils,
    }
)

# set an index, as code below will need it; this also removes default ID
df.set_index("uniquekey", inplace=True)

# pangres / sqlalchemy upsert

# CREATE TABLE "events" (
# "uniquekey" TEXT primary key,
#   "link" TEXT,
#   "title" TEXT,
#   "startdate" TEXT,
#   "enddate" TEXT,
#   "council" TEXT,
#   "updateddate" DATETIME DEFAULT CURRENT_TIMESTAMP
# );

engine = create_engine("sqlite:////tmp/gs.sqlite")

upsert(con=engine, df=df, table_name="events", if_row_exists="update", dtype=None)

# delete from database where enddate < today (this really should be registration date, when available)

from sqlalchemy import text

deleteExpiredSQL = f"delete from events where enddate<'{str(datetime.now())}'"

with engine.connect() as connection:
    result = connection.execute(text(deleteExpiredSQL))
