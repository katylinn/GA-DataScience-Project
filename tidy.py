# -*- coding: utf-8 -*-
"""
Created on Tue Nov 24 16:43:59 2015

@author: Katy Linn

This script parses the NEEMO 20 log files into a timestamped event log, annotates the data
set with eva and time delay data, and saves the tidy data set into a csv for further 
analysis.

STEP 1:  Parse EVA JSON into "evas" list
STEP 2:  Parse Communications delay JSON into "delays" list
STEP 3:  Parse through log files with the help of "fix lines" function which accounts for 
some issues with the recording tool
"""

#Helper Library to to loop through files in a folder
import os
#Regular Expression Library
import re
#JSON parsing Library
import json
#Library for working with dates, times, and time deltas
import datetime as dt
#Pandas data science library
import pandas as pd



'''
STEP 1:
Parse through EVA JSON and grab the following info for each EVA:
-Name (String)
-Start (dt.datetime)
-End (dt.datetime)

Data is stored as a list of dictionaries in a variable called "evas"

*3 hour offset is to account for timezone
'''
evas = []

with open("NEEMO20-Crew/constraints.eva.json", "rt") as file:
    json_dict = json.loads(file.read())
    name = ""
    start = ""
    end = ""
    for elem in json_dict["datapoints"]:
        if name != "":
            end = elem["date"]
            evas.append({
                "name":name,
                "start":dt.datetime.fromtimestamp(start/1000)+ dt.timedelta(hours=3),
                "end":dt.datetime.fromtimestamp(end/1000)+ dt.timedelta(hours=3)
            })
            name = ""
            start = ""
            end = ""
        elif "value" in elem:
            name = elem["value"]
            start = elem["date"]

'''
STEP 2:
Parse through Communications Delay JSON and grab the following info for each EVA:
-Name (String)
-Start (dt.datetime)
-End (dt.datetime)
-Delay Minutes (int)

Data is stored as a list of dictionaries in a variable called "delays"

*3 hour offset is to account for timezone
'''
#Helper function to determine actual delay from "Name"
def calc_delay(name):
    if name[-14:] == "REAL TIME COMM":
        return 0
    elif name[-8:] == "(5 mins)":
        return 5
    elif name[-9:] == "(10 mins)":
        return 10
    else:
        return -1

delays = []

with open("NEEMO20-Crew/constraints.comm_latency.json", "rt") as file:
    json_dict = json.loads(file.read())
    name = ""
    start = ""
    end = ""
    for elem in json_dict["datapoints"]:
        if name != "":
            end = elem["date"]
            delays.append({
                "name":name,
                "start":dt.datetime.fromtimestamp(start/1000) + dt.timedelta(hours=3),
                "end":dt.datetime.fromtimestamp(end/1000) + dt.timedelta(hours=3),
                "delay": calc_delay(name)
            })
            name = ""
            start = ""
            end = ""
        elif "value" in elem:
            name = elem["value"]
            start = elem["date"]
'''
STEP 3:
Parse through log files and grab the following info for each user interaction (event):
-time (dt.datetime)
-clientAddress (string)
-browser (string)
-event (string)
-other (string)

Annotate each event with:
-is_eva (bool)
-eva_name (string)
-delay (int)
*this data comes from the data parsed above
'''
#helper function to overcome some bugs in the recording extension
def fix_lines(iterable):
        
    # a small number of files are slightly corrupted and contain multiple listings of 
    # "recordingId=..." several times throughout the file appended to the END of otherwise
    # well behaved lines.  This casuses a problem when the parser expects the rest of the 
    # line to be parsed into something meaningful, like a time delta.  However, in all 
    # cases, the recording Id is consistent with the rest of the file, and represented in
    # the filename itself.  So, all instances of recordingId are eliminated from the file
    # and rows are instead annotated with the appropriate substring of the file name
    temp_list = []
    itr = iter(iterable)
    for line in itr:
        temp_list.append(re.sub(r'recordingId=[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{3}Z_[0-9]*', "", line))

    itr = iter(temp_list)  # get an iterator
    
    # a much more prevalent issue in the log files is the existence of unescaped newlines 
    # in "text" elements.  This code borrows from a stack overflow addressing a similar
    # problem, though has been highly adapted to this specific case.
    #http://stackoverflow.com/questions/19674980/cleaning-a-tab-delimited-file-with-unescaped-newlines
    while True:
        line = next(itr)
        cur = ""
        #catch the cases with text, and check for the unescaped newlines
        if line[0:5] == "text,":
            #the actual string argument is the 5th column
            text = line.split(",")[4]
            #keep collecting lines til you find the 
            #next one that ends with the single quote
            while text[-2:] != "'\n":
                if cur == "":
                    cur = line
                else:
                    cur = cur.rstrip('\r') + text
                text=next(itr)
            else:
                if cur == "":
                     #this is the case where there was no unintentional linebreak
                    cur = line
                else:
                     #this is the case where we collected the strings above
                    cur = cur.rstrip('\r') + text
            yield cur
            cur = ""
        else:
            #all the strings that don't start with "text," are well behaved, so pass along
            yield line
                


# This loop is the main work horse of the file.  Loops through all the log files in the 
# specified folder reading one line at a time, and parsing the information into a 
# timestamped log of events.  The data is then written to a tidy.csv file for further 
# processing

# name of folder containing log files
path = "NEEMO20-Crew"

# lists proved to be much faster than data frames, especially when constructing and 
# appending rows in a loop.  So, lists are used as temporary objects in order to reduce
# concat and append operations

# dfs is a list of dataframes, one for each log file
dfs = []

#iterate through each file in the given folder looking for logs
for filename in os.listdir(path):
    if filename.endswith(".log") and filename.startswith("2015"): 
        #iterate through each log fie
        with open(path+"/" + filename, "rt") as file:
            # This loop takes a while to run.  Printing filename to console is just a way
            # to show that progress is being made.
            print filename
            
            # dl is a temporary list used to hold all the events for the given file.
            # if is converted to a data frame at the end of file, and appended to dfs
            dl = []
            
            # for reasons explained in the "fix_lines" function, we ignore the recordingId
            # in the file and use the filename (stripping off ".log")
            recordingId = filename[:-4]
            
            #set up some variables to contain meta info for each event
            clientAddress = ""
            t_init = dt.datetime(2000,1,1)
            t = dt.datetime(2000,1,1)
            browser= ""
            
            # fix_lines gives us an iterater that works around some of the issues in the 
            # recording tool 
            for line in fix_lines(file):
            
                #parse initial timestamp and set current t
                if line[0:2] == "t=":
                    t_init = dt.datetime.strptime(line[2:-2], "%Y-%m-%dT%H:%M:%S.%f")
                    t = t_init
                elif line[0:2] == "t+":
                    #every subsequent "t+" line represents a time delta, so update current
                    t = t+dt.timedelta(milliseconds = int(line[2:]))

                
                #ignoring these for now
                elif line[0:3] == "DOM":
                    pass
                elif line[0:6] == "window":
                   pass
                

                elif line[0:7] == "browser":
                    browser = line[8:-1]
                elif line[0:13] == "clientAddress":
                    clientAddress = line[14:-1]
                    
                #everything else is something we want to parse as an event
                else:
                    #create dict with meta info  
                    event = {
                        "recordingId": recordingId,
                        "clientAddress": clientAddress,
                        "browser": browser,
                        "t":t
                    
                    }
                    # check if event occurred during eva, and if so, include meta info
                    is_eva = False
                    eva_name = ""
                    for eva in evas:
                        if eva["start"] < t and t < eva["end"]:
                            is_eva = True
                            eva_name = eva["name"]
                            break
                    event.update({
                        "is_eva":is_eva,
                        "eva_name": eva_name
                    })
                    
                    # look up what delay was in effect at the time of the event
                    delay_min = 0
                    for delay in delays:
                        if delay["start"] < t and t < delay["end"]:
                            delay_min = delay["delay"]
                            break
                    event.update({
                        "delay":delay_min
                    })
                    
                    ### This could obviously be optimized for both performance and number of
                    ### lines of code.  Performance gains would be minimal, (the if/elif is 
                    ### "fast enough") and this method is exceedingly "readable"
                    ### Would reconsider optimizing if it were to be used in a production 
                    ### environment 
                    
                    # check against our known list of event keywords and parse out event 
                    # "other" arguments
                    if line[0:6] == "scroll":
                        event.update({
                            "event":"scroll",
                            "other":line[7:-1]
                        })
                        dl.append(event)
                    elif line[0:5] == "click":
                        event.update({
                            "event":"click",
                            "other":line[6:-1]
                        })
                        dl.append(event)
                    elif line[0:3] == "att":
                        event.update({
                            "event":"att",
                            "other":line[4:-1]
                        })
                        dl.append(event)
                    elif line[0:10] == "mouse,over":
                        event.update({
                            "event":"mouse,over",
                            "other":line[11:-1]
                        })
                        dl.append(event)
                    elif line[0:9] == "mouse,out":
                        event.update({
                            "event":"mouse,out",
                            "other":line[10:-1]
                        })
                        dl.append(event)
                    elif line[0:5] == "state":
                        event.update({
                            "event":"state",
                            "other":line[6:-1]
                        })
                        dl.append(event)
                    elif line[0:3] == "tap":
                        event.update({
                            "event":"tap",
                            "other":line[4:-1]
                        })
                        dl.append(event)
                    elif line[0:3] == "key":
                        event.update({
                            "event":"key",
                            "other":line[4:-1]
                        })
                        dl.append(event)
                    elif line[0:4] == "text":
                        event.update({
                            "event":"text",
                            "other":line[5:-1]
                        })
                        dl.append(event)
                    elif line[0:13] == "drag,dragging":
                        event.update({
                            "event":"drag,dragging",
                            "other":line[14:-1]
                        })
                        dl.append(event)
                    elif line[0:15] == "dragend,endDrag":
                        event.update({
                            "event":"dragend,endDrag",
                            "other":line[16:-1]
                        })
                        dl.append(event)
                    elif line[0:15] == "touch,touchmove":
                        event.update({
                            "event":"touch,touchmove",
                            "other":line[16:-1]
                        })
                        dl.append(event)
                    elif line[0:16] == "touch,touchstart":
                        event.update({
                            "event":"touch,touchstart",
                            "other":line[17:-1]
                        })
                        dl.append(event)
                    elif line[0:14] == "touch,touchend":
                        event.update({
                            "event":"touch,touchend",
                            "other":line[15:-1]
                        })
                        dl.append(event)
                    elif line[0:6] == "txtsel":
                        event.update({
                            "event":"txtsel",
                            "other":line[7:-1]
                        })
                        dl.append(event)
                    elif line[0:4] == "open":
                        event.update({
                            "event":"open",
                            "other":line[5:-1]
                        })
                        dl.append(event)
                    elif line[0:14] == "session-closed":
                        event.update({
                            "event":"session-closed",
                            "other":line[15:-1]
                        })
                        dl.append(event)
                    elif line[0:4] == "hold":
                        event.update({
                            "event":"hold",
                            "other":line[5:-1]
                        })
                        dl.append(event)
                    elif line[0:5] == "paste":
                        event.update({
                            "event":"paste",
                            "other":line[6:-1]
                        })
                        dl.append(event)
                    elif line[0:3] == "cut":
                        event.update({
                            "event":"cut",
                            "other":line[4:-1]
                        })
                        dl.append(event)
                    elif line[0:4] == "prop":
                        event.update({
                            "event":"prop",
                            "other":line[5:-1]
                        })
                        dl.append(event)
                    elif line == "\n" or line == "":
                        #all the files end with a blank line
                        pass
                        
                    else:
                        print line
            # at the end of each file, create put the events in a data frame and append to 
            # dfs
            if len(dl) > 0:
                dfs.append(pd.DataFrame(dl))

# concatenate all the dataframes and write out to file
df = pd.concat(dfs)
df.to_csv("tidy.csv")
