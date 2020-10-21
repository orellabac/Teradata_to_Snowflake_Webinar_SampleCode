#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import configparser
import threading
import queue
import snowflake.connector
import time
import json
import os
import fileinput
import re
import argparse
from datetime import datetime
from os import path
import math
from collections import Counter
import mmap
import getpass



def thread_function(con, index, max_stmnt, stmnt_q, created_q, failed_q, done_q):
    
    cur = con.cursor()    
    ebuf = []

    cur.execute("set quoted_identifiers_ignore_case = TRUE")
    cur.execute("alter session set TIMESTAMP_TYPE_MAPPING = TIMESTAMP_LTZ")

    while True:
       
      if (stmnt_q.qsize() > 0) and ((created_q.qsize() + failed_q.qsize()) < max_stmnt):  
        
          stmnt = stmnt_q.get()

          try:
    
              cur.execute(stmnt)
              created_q.put(stmnt)

          except snowflake.connector.errors.ProgrammingError as e:
              
              ebuf.append({"error_msg":e, "statement":stmnt})          
              failed_q.put(ebuf[len(ebuf)-1]) 

      else:
      
          break
          
    cur.close()

    done_q.put(index)
        
    return


    
def msg_thread_function(parallelism, msg_freq, session_id, no_of_stmnts, created_q, failed_q, done_q):
        
    run_dict = {
                "start_time": datetime.now().strftime("%d/%m/%Y %H:%M:%S"), 
                "session_id": session_id,
                "number_of_statements": no_of_stmnts,
                "number_of_created": 0,
                "number_of_failed": 0,
                "end_time": ""
               }
    
    c = 0
    f = 0
    
    print("                   Start time: ", run_dict["start_time"], "\n")
    print("                   Session ID: ", run_dict["session_id"])
    print("                  Parallelism: ", '{:5d}'.format(parallelism))
    print("    Statements in Current Run: ", '{:5d}'.format(run_dict["number_of_statements"]))
    print("                Total Created: ", '{:5d}'.format(c), " Failed In Run:", '{:5d}'.format(f), end = "\r", flush = True)
    
    time.sleep(msg_freq)
    
    while done_q.qsize() < parallelism:
    
      c = created_q.qsize()
      f = failed_q.qsize()  
      print("                Total Created: ", '{:5d}'.format(c), " Failed In Run:", '{:5d}'.format(f), end = "\r", flush = True) 
      time.sleep(msg_freq)
    
    time.sleep(2)
    run_dict["number_of_created"] = created_q.qsize()
    run_dict["number_of_failed"] = failed_q.qsize()
    run_dict["end_time"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    print("                Total Created: ", '{:5d}'.format(run_dict["number_of_created"]), " Failed In Run:", '{:5d}'.format(run_dict["number_of_failed"]), end = "\r", flush = True)
    print("\n")
    print("            End time: ", run_dict["end_time"])
    print("\n")
    

    #print("test")
    #print(run_dict["session_id"])
    time.sleep(20)
    while not done_q.empty():
        #print(done_q.get(), end=" ")
        done_q.get()

    done_q.put(run_dict)

    return



def remove_error_msg(msg):
    
    ret = msg

    i = msg.find("<sc")

    j = msg.find("</sc")

    k = msg[i:j].find("Error")

    if k > -1: ret = msg[0:i+k] + msg[j:] 
    
    i = ret.find("<sc")
    j = ret.find("</sc")
    k = ret[i:j].rfind("\n")
    
    if k != -1:
       
       ret = ret[0:k] + ret[k+1:] 
        
    return ret



def init(input_directory):
    #creates queue object with all statements
    stmnt_q = queue.Queue()

    for item in os.listdir(input_directory):
        if os.path.isdir(os.path.join(input_directory,item)) and item not in exclude_dirs:
            for dirpath, dirnames, files in os.walk(os.path.join(input_directory,item)):
                #print(f'Found directory: {dirpath}')
                for file_name in files:
                    fname, fextension = os.path.splitext(file_name)
                    fextension = fextension.lower()
                    if (fextension == ".sql"):
                        print("Processing file " + dirpath + os.sep + file_name)
                        f = open(dirpath + os.sep + file_name)
                        stmnt_q.put(f.read())
                        f.close()
    return stmnt_q


def calc_par(no_of_stmnts, parallelism):
    
    ret = parallelism
    
    if (no_of_stmnts/30) <= parallelism:
        
       ret = math.ceil(no_of_stmnts/30)

    return ret



def decode_error(argument):
    
    switcher = { 
        603: "PROCESS_ABORTED_DUE_TO_ERROR", 
        900: "EMPTY_SQL_STATEMENT", 
        904: "INVALID_IDENTIFIER",
        939: "TOO_MANY_ARGUMENTS_FOR_FUNCTION", 
        979: "INVALID_GROUP_BY_CLAUSE", 
        1002: "SYNTAX_ERROR_1", 
        1003: "SYNTAX_ERROR_2", 
        1007: "INVALID_TYPE_FOR_PARAMETER", 
        1038: "CANNOT_CONVERT_PARAMETER", 
        1044: "INVALID_ARG_TYPE_FOR_FUNCTION", 
        1104: "COLUMN_IN_SELECT_NOT_AGGREGATE_OR_IN_GROUP_BY", 
        1789: "INVALID_RESULT_COLUMNS_FOR_SET_OPERATION", 
        2001: "OBJECT_DOES_NOT_EXIST_1", 
        2003: "OBJECT_DOES_NOT_EXIST_2",  
        2016: "EXTRACT_DOES_NOT_SUPPORT_VARCHAR",  
        2022: "MISSING_COLUMN_SPECIFICATION",
        2025: "DUPLICATE_COLUMN_NAME",
        2026: "INVALID_COLUMN_DEFINITION_LIST",
        2028: "AMBIGUOUS_COLUMN_NAME",
        2262: "DATA TYPE MISMATCH WITH DEFAULT VALUE",
        2040: "UNSUPPORTED_DATA_TYPE",
        2140: "UNKNOWN_FUNCTION",
        2141: "UNKNOWN_USER_DEFINED_FUNCTION",
        2143: "UNKNOWN_USER_DEFINED_TABLE_FUNCTION", 
        2151: "INVALID_COMPONENT_FOR_FUNCTION_TRUNC",
        2212: "MATERIALIZED_VIEW_REFERENCES_MORE_THAN_1_TABLE",
        2401: "LIKE_ANY_DOES_NOT_SUPPORT_COLLATION",  
        2402: "LTRIM_WITH_COLLATION_REQUIRES_WHITESPACE_ONLY",  
        90105: "CANNOT_PERFORM_CREATE_VIEW",
        90216: "INVALID_UDF_FUNCTION_NAME"         
    } 
  
    return switcher.get(argument, "nothing") 



def main(input_script):
    
    if not os.path.exists(out_path):
        os.makedirs(out_path)

    global parallelism

    con = snowflake.connector.connect (
          account   = sf_account,
          user      = sf_user,
          password  = sf_password,
          warehouse = sf_warehouse)
   
    stmnt_q = init(input_script)    
    created_q = queue.Queue()
    failed_q = queue.Queue()
    done_q = queue.LifoQueue()
    
    no_of_stmnts = stmnt_q.qsize()
    
    exit = 0
    tot_created_last_run_end = 0

    stmnt_q_cur_run = stmnt_q
    no_of_stmnts_cur_run = no_of_stmnts

    run_num = 1

    while True:

        print("\n")
        print("Recursive Run", run_num, "...")

        parallelism = calc_par(no_of_stmnts_cur_run, parallelism)

        threads = list()

        for index in range(parallelism):
            x = threading.Thread(target=thread_function, args=(con, index, max_stmnt, stmnt_q_cur_run, created_q, failed_q, done_q, ))
            threads.append(x)
            x.start()

        x = threading.Thread(target=msg_thread_function, args=(parallelism, msg_freq, con.session_id, no_of_stmnts_cur_run, created_q, failed_q, done_q, ))
        threads.append(x)
        x.start()

        for index, thread in enumerate(threads):
            thread.join()

        if failed_q.qsize() == 0: 
            print("All objects successfully created.")
            con.close()
            break

        if created_q.qsize() == tot_created_last_run_end:
            print("No new objects created in previous run. Ending recursive runs.")
            con.close()
            break

        tot_created_last_run_end = created_q.qsize()

        stmnt_q_cur_run = queue.Queue()

        for stmnt in list(failed_q.queue):    
            y = remove_error_msg(stmnt["statement"])
            stmnt_q_cur_run.put(y)

        no_of_stmnts_cur_run = failed_q.qsize()
        failed_q = queue.Queue()
        run_num = run_num + 1

    print("Creating output files...")
    
    execution_summary = done_q.get()

    f = open(out_path + "created_" + str(execution_summary["session_id"]) + ".sql","w")
    
    for stmnt in list(created_q.queue):    
        f.write(remove_error_msg(stmnt) + "\n")
    f.close()    
    
    errno_dict = {}
    error_list = []
    
    f = open(out_path + "failed_" + str(execution_summary["session_id"]) + ".sql","w")
    for em in list(failed_q.queue):
        
        f.write(remove_error_msg(em["statement"]))  
        f.write("\n")
                
        if errno_dict.get(em["error_msg"].errno) != None:
               
            error_file = errno_dict.get(em["error_msg"].errno)
              
        else:
            error_name = str(decode_error(em["error_msg"].errno))
            error_file = open(out_path + "error_" + str(execution_summary["session_id"]) + "_" + error_name + ".sql", "w+")
            errno_dict[em["error_msg"].errno] = error_file
        
        i = em["statement"].find("</sc")
        error_file.write(em["statement"][0:i] + "\n")
        error_file.write('Error {0} ({1}): {2} ({3})'.format(em["error_msg"].errno, em["error_msg"].sqlstate, em["error_msg"].msg, em["error_msg"].sfqid))
        error_file.write(em["statement"][i:])
    

    for stmnt in list(failed_q.queue):
        error_name = str(decode_error(stmnt["error_msg"].errno))
        error_list.append(error_name)

    freq = Counter(error_list)
        
    f = open(out_path + "error_summary_" + str(execution_summary["session_id"]) + ".txt", "w+")
    f.write(str(freq))
    f.close()

    for key in errno_dict:
        
        errno_dict[key].close() 
    
    f = open(out_path + "execution_summary_" + str(execution_summary["session_id"]) + ".json", "w+")
    f.write(json.dumps(execution_summary))
    f.close()
    
    os.system("type " + out_path + "created_" + str(execution_summary["session_id"]) + ".sql" + " >> " + out_path + "created.sql")
    
    print("Done")
    
    return

if __name__ == "__main__":

    print("Deploying objects to Snowflake...")
    print("====================================")
    
    parser = argparse.ArgumentParser()
    parser.add_argument("InPath",  help = "Path for SQL scripts")
    parser.add_argument("Account", help = "Snowflake Account")
    parser.add_argument("Warehouse", help = "Snowflake Warehouse")
    parser.add_argument("Role", help = "Snowflake Role")
    parser.add_argument("User", help = "Snowflake User")
    parser.add_argument("--Password", help = "Snowflake Password", required=False)

    args = parser.parse_args()

    if args.Password is None:
        try:
            args.Password = getpass.getpass()
        except:
            print("Error reading password")
            exit(1)

    if path.exists(args.InPath):
      input_script = args.InPath  
    else:
       
      print("Input Path for SQL scripts does not exist.") 
      sys.exit(0)
    
    config = configparser.ConfigParser()
    
    out_path    = "./out/" 
    msg_freq    = 10
    max_stmnt   = sys.maxsize
    parallelism = 5
    exclude_dirs= ["out","schema","extensions","reports","logs"]

      
    sf_account   = args.Account
    sf_user      = args.User
    sf_password  = args.Password
    sf_warehouse = args.Warehouse
    sf_role      = args.Role

    main(input_script)  
    sys.exit(0)
    