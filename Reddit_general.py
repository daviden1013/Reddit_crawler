# -*- coding: utf-8 -*-
import scrapy
from bs4 import BeautifulSoup
import pyodbc
from datetime import datetime
 
 
class RedditSpider(scrapy.Spider):
    name = "general"
    boardName = "AskReddit"
    urls = []
    
    def __init__(self):
        unit = int(7 * 24 * 3600) # a week as one unit
        
        start = 1357028296
        end = 1493626696
        partsNum = int(end - start)//unit

        for i in range(partsNum):
            self.urls.append("https://www.reddit.com/r/" + self.boardName + "/search?q=timestamp%3A" + str(start + i*unit) + ".." + str(start + (i+1)*unit) + "&sort=new&restrict_sr=on&syntax=cloudsearch")
            print(self.urls[i])
            
        self.urls.append("https://www.reddit.com/r/" + self.boardName + "/search?q=timestamp%3A" + str(end - (int(end - start)%unit)) + ".." + str(end) + "&sort=new&restrict_sr=on&syntax=cloudsearch")
    
    def start_requests(self):
        for url in self.urls:
            yield scrapy.Request(url, self.parse, meta={
                'splash': {
                'endpoint': 'render.html',
                'args': { 'wait': 0.5 }
                }
            })


    def parse(self, response):
        print(response.url)
        
        soup = BeautifulSoup(response.body, 'html.parser')
        self.read(soup)
        #parse html for next link
        mydivs = soup.findAll("a", { "rel" : "nofollow next" })
        if len(mydivs) != 0:
            link = mydivs[0]['href']
            print(link)  
            yield scrapy.Request(link, self.parse, meta={
                'splash': {
                'endpoint': 'render.html',
                'args': { 'wait': 6.0 }
                }
            })
        else:
            with open("report.html", 'wb') as f:
                f.write(response.body)
            
    def read(self, soup):
        print("trying...")
        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            r'DBQ=C:\Users\david_000\AppData\Local\Programs\Python\Python35\Scripts\tutorial\reddit.accdb;' 
        )
        cnxn = pyodbc.connect(conn_str)
        cursor = cnxn.cursor()
        print("connected")
        
        for post in soup.findAll("div", {"class" : "contents"})[0].children:
            
            titlesEle = post.findAll("a", { "class" : "search-title may-blank" })[0]
            pointEle = post.findAll("span", { "class" : "search-score" })[0]
            commentEle = post.findAll("a", {"class" : "search-comments may-blank"})[0]
            timeEle = post.findAll("time")[0]
            authorEle = post.findAll("span", { "class" : "search-author" })[0]
            contentEles = post.findAll("div", {"class" : "md"})
            if len(contentEles) != 0:
                contentEle = post.findAll("div", {"class" : "md"})[0]
                content = contentEle.text.replace("'", "''")
            else:
                content = ""
            
            title = titlesEle.text.replace("'", "''")
            point = pointEle.text.replace(" point", "")
            point = point.replace("s", "")
            comment = commentEle.text.replace(" comment", "")
            comment = comment.replace("s", "")
            time = timeEle['datetime'][0:10].replace("-", "")
            author = authorEle.text[3:]
            
            SQL = ("INSERT INTO " + self.boardName + " (title, point, comment, haha, author, content) VALUES (" 
                  "'" + title + "'" 
                  ",'" + point + "'" 
                  ",'" + comment + "'" 
                  ",'" + time + "'" 
                  ",'" + author + "'" 
                  ",'" + content + "'" 
                  ")")
                           
            cursor.execute(SQL)
            cnxn.commit()
        
        cursor.close()
        cnxn.close()
