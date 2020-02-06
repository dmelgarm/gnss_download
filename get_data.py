#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 15:42:58 2020

@author: dmelgarm
"""

import requests
from bs4 import BeautifulSoup
from pathlib import Path
from zipfile import ZipFile
import gzip
from glob import glob
from obspy import Stream,Trace,UTCDateTime
from numpy import genfromtxt
from datetime import datetime, timedelta



#########        Things you need to know     #############
station='P540'
search_url='http://geodesy.unr.edu/gps_timeseries/kenv/'
path_out = '/Users/dmelgarm/Parkfield_slowslip/unr_5min/'
dt=300. #sample rate of the data
##########################################################

##########       What do you want to do?     #############
download = True
unpack_all = True
make_mseed = True
##########################################################


#define some functions I will need
def get_url_paths(url, ext='', params={}):
    '''
    url is where I'm looking
    ext is the extension of the files I want URLS for, '.kenv' in the UNR case
    
    stole (borrowed?) this from:
        https://stackoverflow.com/questions/11023530/python-to-list-http-files-and-directories
    
    '''
    response = requests.get(url, params=params)
    if response.ok:
        response_text = response.text
    else:
        return response.raise_for_status()
    soup = BeautifulSoup(response_text, 'html.parser')
    parent = [url + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]
    return parent

def seconds2hours_and_minutes_and_seconds(sec):
    d = datetime(1,1,1) + timedelta(seconds=sec)

    days=d.day-1
    hours=d.hour
    minutes=d.minute
    seconds=d.second
    
    return days,hours,minutes,seconds



# Download yearly zip files
if download == True:
    
    #get the URL listing of avialble YEARLY files
    yearly_urls = get_url_paths(search_url+station+'/', '.kenv.zip')
    
    #create station folder (if it doesn't already exist)
    station_path = path_out+station+'/'
    Path(station_path).mkdir(exist_ok=True)
    
    #download all zip files to station path
    for kurl in range(len(yearly_urls)):
        
        #What's the filename gonna be?
        file_name = yearly_urls[kurl].split('/')[-1]
        
        #Give output so it looks like you're busy
        print('Working on downloading '+file_name)
        
        #request data from url
        r = requests.get(yearly_urls[kurl])

        #write it to the path and specified filename
        with open(station_path+file_name, 'wb') as f:
                f.write(r.content)
    
    
    
#Unpack yearly zip files into daily text files
if unpack_all == True:
    
    # where's the work at?
    station_path = path_out+station+'/'
    
    #How many zip files are we talking about?
    file_list = glob(station_path+'*.zip')
    
    #unzip 'em
    for f in file_list:
        
        #What file will the unzipped results go into?
        year = f.split('/')[-1].split('.')[1]
        targetdir=station_path+year
        
        #print output, don't want to be called lazy
        print('Unzipping '+f+' to '+targetdir)
        
        #Alrighty, no do it
        with ZipFile(f,'r') as zip_ref:
            zip_ref.extractall(targetdir)
            
    #But you are not done yet, go into the yearly fodlers and unpack EVERY FRIGGIN DAY
    # note that these are now .gz archives, not .zip as the yearly files because why not
    file_list = glob(station_path+'????')
    
    #loop over years
    for folder in file_list:
        
        daily_file_list = glob(folder+'/*.gz')
        
        #loop over days
        for daily_file in daily_file_list:
            
            #What file will the unzipped results go into?
            day_of_year = daily_file.split('/')[-1].split('.')[2]
            targetdir = folder + '/' + day_of_year+'.kenv'
            
            #print output again for sanity checks
            print('Extracting '+daily_file+' to '+targetdir)
                
            infile = gzip.GzipFile(daily_file, 'rb')
            gzip_file_content = infile.read()
            infile.close()

            #Now write it back out in uncompressed form
            outfile = open(targetdir, 'wb')
            outfile.write(gzip_file_content)
            outfile.close()
                        
            
# Make a single mini seed file
if make_mseed == True:
    
    # where's the work at?
    station_path = path_out+station+'/'
    
    #How many years?
    folder_list = glob(station_path+'????') 
    
    #initalzie stream objects
    e=Stream()
    n=Stream()
    z=Stream()
    
    #go into each year, then read one day at a time and make it a trace, then add
    #tot he stream object
    
    for folder in folder_list:
        
        file_list = glob(folder+'/???.kenv')
        print('Working on folder '+folder)
        
        #loop over daily files
        for daily_file in file_list:
            
            #time information
            date_data = genfromtxt(daily_file,usecols=[3,4,5,7],skip_header=1)
            year=date_data[:,0].astype('int')
            month=date_data[:,1].astype('int')
            day=date_data[:,2].astype('int')
            seconds_of_day=date_data[:,3].astype('int')
            
            #data information
            position_data = genfromtxt(daily_file,usecols=[8,9,10],skip_header=1)
            edata=position_data[:,0]
            ndata=position_data[:,1]
            zdata=position_data[:,2]
            
            #ok, we're gonna do something weird here to deal with gaps, each single 300s 
            #epoch of data will be added as an individual trace to a daily stream object
            #with it's own starttime. Then at the end they will be merged into a single
            #daily stream with some nominal value to indicate any data gaps
            e_daily=Stream()
            n_daily=Stream()
            z_daily=Stream()
            for kepoch in range(len(edata)):
                e_trace=Trace()
                n_trace=Trace()
                z_trace=Trace()
                
                #Add the data
                e_trace.data=array([edata[kepoch]])
                n_trace.data=array([ndata[kepoch]])
                z_trace.data=array([zdata[kepoch]])
                
                #Add the sample rate
                e_trace.stats.delta=dt
                n_trace.stats.delta=dt
                z_trace.stats.delta=dt
                
                #Add origin time of that day IT'S IN GPS TIME NOT UTC!!!!!!
                #convert seconds of day to hours and minutes
                days,hours,minutes,seconds=seconds2hours_and_minutes_and_seconds(int(seconds_of_day[kepoch]))
                
                #now make UTC string object
                t0=UTCDateTime(str(year[0])+'-'+str(month[0])+'-'+str(day[0])+'T'+str(hours)+':'+str(minutes)+':00.0')
                e_trace.stats.starttime=t0
                n_trace.stats.starttime=t0
                z_trace.stats.starttime=t0
                
                #Add to stream
                e_daily.append(e_trace)
                n_daily.append(n_trace)
                z_daily.append(z_trace)
            
            #Merge the dailies
            e_daily.merge(fill_value=0)
            n_daily.merge(fill_value=0)
            z_daily.merge(fill_value=0)
                
            #add the daily to the master stream
            e.append(e_daily[0])
            n.append(n_daily[0])
            z.append(z_daily[0])

            
    #Final merge
    e.merge(fill_value=0)
    n.merge(fill_value=0)
    z.merge(fill_value=0)
    
    #Write to file
    out=station_path+station+'.LYE.mseed'
    e.write(out,format='MSEED')
    out=station_path+station+'.LYN.mseed'
    n.write(out,format='MSEED')
    out=station_path+station+'.LYZ.mseed'
    z.write(out,format='MSEED')
                
                
                
            
                

                
            
            
                
                
    