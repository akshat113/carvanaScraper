#Py file to scrape auto companies like carvana, carmax, sonic automotive, lithia motors, rusha enterprises
#with the data scraped we were able to forecast daily sales of the company

from bs4 import BeautifulSoup
import requests
import re
import time
import json
import threading
import math
from datetime import date
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool as ThreadPool
import pymongo
from pymongo import MongoClient
import pandas as pd
from random import randint
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import random
import us_state_abbrev
from sqlalchemy import create_engine
import psycopg2
import io
engine = create_engine(
    'postgresql+psycopg2://postgres:SUNSET113@database-1.calo67onceop.us-east-1.rds.amazonaws.com:5432/untitled')
import ast
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=100)
user_agent_rotator = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'}
headers = {'content-type': 'text/html'}

client = pymongo.MongoClient(
    "mongodb+srv://vivek:guzman@cluster0-5vrxh.mongodb.net/test?authSource=admin&replicaSet=Cluster0-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true")

db = client.Auto
collection = db.ScrapedInventory


def scraperRush():
    db = client.Auto
    collection = db.ScrapedInventory
    r = requests.get('https://www.rushtruckcenters.com/api/TrucksSearch?&perpage=10000')
    res = json.loads(r.text)
    trucks = res['TrucksSearchResults']
    for truck in trucks:
        if truck['Price'] == 'Call for price':
            truck['Price'] = 0
        else:
            truck['Price'] = int(truck['Price'].replace(',','').replace('$',''))
        collection.insert_one({'ticker': 'RUSHA', 'scrapedDate': str(datetime.now().date()),'price': truck['Price'], 'vehicleId': truck['StockNumber'], 'state': truck['State'], 'cabType' : truck['CabType'] })
    print(1)
def scraperSonic():
    db = client.Auto
    collection = db.ScrapedInventory
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'}

    r = requests.get('https://www.sonicautomotive.com/all-inventory/index.htm', headers = headers)
    soup = BeautifulSoup(r.text)
    vehicleCount = int(soup.find(class_='vehicle-count').text)
    for i in range(math.ceil(vehicleCount/35)):
        url = 'https://www.sonicautomotive.com/all-inventory/index.htm?start=' + str(i*35)
        r = requests.get(url, headers = headers)
        soup = BeautifulSoup(r.text)
        for vehicle in soup.find_all(class_='item'):
            try:
                price = int(vehicle.find(class_='value').text.replace(',','').replace('$',''))
            except Exception as e:
                price = 0
            vehicleId = 0
            try:
                for dt in vehicle.find(class_='last').find_all('dt'):
                    if 'Stock' in dt.text:
                        vehicleId = dt.next_element.next_element.next_element.text.replace(',', '')
            except Exception as e:
                vehicleId = 0
            vehicleUrl = 'https://www.sonicautomotive.com/' + vehicle.find(class_='url')['href']
            r = requests.get(vehicleUrl, headers = headers)
            soup = BeautifulSoup(r.text)
            state = vehicle['data-state']
            year = vehicle.find('div')['data-year']
            make = vehicle.find('div')['data-make']
            collection.insert_one(
                {'ticker': 'SAH', 'scrapedDate': str(datetime.now().date()), 'price': price,
                 'vehicleId': vehicleId, 'state': state, 'make':make, 'year': year })
def lithiapagescraper(ctr):
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36'
    }
    url = 'https://www.lithia.com/all-inventory/index.htm?start=' + str(ctr * 16)
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text)
    for vehicle in soup.find_all(class_='item'):
        try:
            price = int(vehicle.find(class_='value').text.replace(',', '').replace('$', ''))
        except Exception as e:
            price = 0
        vehicleId = 0
        try:
            for dt in vehicle.find(class_='last').find_all('dt'):
                if 'Stock' in dt.text:
                    vehicleId = dt.next_element.next_element.next_element.text.replace(',', '')
        except Exception as e:
            vehicleId = 0
        state = vehicle['data-state']
        year = vehicle.find('div')['data-year']
        make = vehicle.find('div')['data-make']
        collection.insert_one(
            {'ticker': 'LAD', 'scrapedDate': str(datetime.now().date()), 'price': price,
             'vehicleId': vehicleId, 'state': state,'make':make, 'year': year })
def scraperLithia():
    db = client.Auto
    collection = db.ScrapedInventory
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'}

    r = requests.get('https://www.lithia.com/all-inventory/index.htm', headers=headers)
    soup = BeautifulSoup(r.text)
    vehicleCount = int(soup.find_all(class_='vehicle-count')[1].text)
    for page in range(math.ceil(math.ceil(vehicleCount/16)/25)):
        temppages = (page * 25, page * 25 + 1, page * 25 + 2, page * 25 + 3, page * 25 + 4, page * 25 + 5, page * 25 + 6,page * 25 + 7, page * 25 + 8, page * 25 + 9, page * 25 + 10, page * 25 + 11, page * 25 + 12, page * 25 + 13,page * 25 + 14, page * 25 + 15, page * 25 + 16, page * 25 + 17, page * 25 + 18, page * 25 + 19, page * 25 + 20,page * 25 + 21, page * 25 + 22, page * 25 + 23, page * 25 + 24)
        # Used multithreading here to increase the speed of web scraping. It simultaneously scrapes 50 cars data. It reduced the total scrape timme from 3hrs to 15 minutes
        pool = ThreadPool(25)
        pool.map(lithiapagescraper, temppages)
def scraperPenske():
    db = client.Auto
    collection = db.ScrapedInventory
    #thread1 = threading.Thread(target=scraperPenske2)
    #thread1.start()
    #thread2 = threading.Thread(target=scraperPenske3)
    #thread2.start()
    #thread3 = threading.Thread(target=scraperPenske4)
    #thread3.start()
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'}
    for i in range(800):
        try:
            try:
                r = requests.get('https://www.penskecars.com/inventory.aspx?_page=' + str(i+1), headers = headers)
                print('https://www.penskecars.com/inventory.aspx?_page=' + str(i+1))
                soup = BeautifulSoup(r.text)
            except Exception as e:
                print(str(e))
            try:
                for car in soup.find(id='srp-inventory-cards').find_all(class_='srp-card'):
                    price = 0
                    try:
                        vehicleId = int(car.find(class_='button-row-compare').find('a')['id'])
                        price = car.find(class_='srp-card-body').find('h3').text.replace(',', '').replace('$', '').lstrip().rstrip()
                    except Exception as e:
                        print(str(e))
                    state = 'Blank'
                    try:
                        collection.insert_one(
                            {'ticker': 'PAG', 'scrapedDate': str(datetime.now().date()), 'price': price,
                             'vehicleId': vehicleId, 'state': state})
                    except Exception as e:
                        print(str(e))
            except Exception as e:
                print(str(e))
        except Exception as e:
            print(str(e))
    print(1)
  
  def cvnalistingpage(ctr):
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36'
    }
    db = client.Auto
    collection = db.ScrapedInventory
    ctr, body, year = ctr.split(',')
    ctr = int(ctr)
    try:
        r = requests.get("https://www.carvana.com/cars/"+body+"/"+year+"?page=" + str(ctr + 1), headers=headers)
        soup = BeautifulSoup(r.text)
        for link in soup.find_all('a', {'data-qa': 'vehicle-link'}):

            r = requests.get('https://apim.carvana.io/vehicle-details-api/api/v1/vehicledetails?vehicleId='+ link['href'][9:], headers= headers)
            res = json.loads(r.text)
            vehicleId = res['header']['vehicleId']
            make = res['header']['make']
            year = res['header']['year']
            price = res['header']['price']
            bodytype = res['header']['bodyType']
            state = res['header']['location']['stateAbbreviation']
            collection.insert_one(
                {'ticker': 'CVNA', 'scrapedDate': str(datetime.now().date()), 'price': price,
                 'vehicleId': vehicleId, 'state': state, 'make':make,'year':year,'body': bodytype})
    except Exception as e:
        print(str(e))
def cvnaScraper():
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36'
    }
    db = client.Auto
    collection = db.ScrapedInventory
    years = ['2009-2016','2017','2018','2019','2020']
    bodytypes = ['Suv','Sedan','Hatchback','Coupe','Pickup','MiniVan','Wagon','Convertible']
    for body in bodytypes:
        for year in years:
            r = requests.get("https://www.carvana.com/cars/"+body+"/"+year, headers=headers)
            soup = BeautifulSoup(r.text)
            count = int(soup.find('div',{'data-qa':'results-count'}).text.split()[1].replace(",",'').lstrip().rstrip())
            for page in range(math.ceil(math.ceil(count/21)/40)):
                temppages = [
                    page * 40, page * 40 + 1, page * 40 + 2, page * 40 + 3, page * 40 + 4, page * 40 + 5, page * 40 + 6,
                    page * 40 + 7, page * 40 + 8, page * 40 + 9, page * 40 + 10, page * 40 + 11, page * 40 + 12,
                    page * 40 + 13,
                    page * 40 + 14, page * 40 + 15, page * 40 + 16, page * 40 + 17, page * 40 + 18, page * 40 + 19,
                    page * 40 + 20,
                    page * 40 + 21, page * 40 + 22, page * 40 + 23, page * 40 + 24, page * 40 + 25, page * 40 + 26,
                    page * 40 + 27,
                    page * 40 + 28, page * 40 + 29, page * 40 + 30, page * 40 + 31, page * 40 + 32, page * 40 + 33,
                    page * 40 + 34,
                    page * 40 + 35, page * 40 + 36, page * 40 + 37, page * 40 + 38, page * 40 + 39]
                for i in range(len(temppages)):
                    temppages[i] = str(temppages[i]) + ',' + body + ',' + year
                pool = ThreadPool(40)
                pool.map(cvnalistingpage, temppages)
