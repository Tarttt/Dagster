import os
import shutil
import time
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor
import urllib.request
from minio import Minio

minio_endpoint = "10.10.0.10:9000"
minio_user = "mtartt"
minio_pass = os.getenv("MinIO_PASS")
bucket_name = 'pilotline-images'

client = Minio(
    minio_endpoint,
    access_key=minio_user,
    secret_key=minio_pass,
    secure=False
    )
 

#Need to list out individual directories to speed up processing time (searching file modified time in folder)
source_folders = [r"X:\\Run_Image",
                 r"Y:\\Run_Image",
                 r"V:\\Run_Image",
                 r"W:\\Run_Image",
                 r"X:\\Run_Log",
                 r"Y:\\Run_Log",
                 r"V:\\Run_Log",
                 r"W:\\Run_Log",
                 r"X:\\Run_Data",
                 r"Y:\\Run_Data",
                 r"V:\\Run_Data",
                 r"W:\\Run_Data"]
                  
destinations = [r"box2\Run_Image",
                r"box3\Run_Image",
                r"box4\Run_Image",
                r"box5\Run_Image",
                r"box2\Run_Log",
                r"box3\Run_Log",
                r"box4\Run_Log",
                r"box5\Run_Log",
                r"box2\Run_Data",
                r"box3\Run_Data",
                r"box4\Run_Data",
                r"box5\Run_Data"]



directories = zip(source_folders, destinations)
last_scan = 'last_scan.txt'


#%%

## update last run
def last_run():
    try:
        with open(last_scan, 'r') as f:
            last_scan_time = float(f.read())
    except FileNotFoundError: 
        last_scan_time = 0.0
    return last_scan_time        
 



    
### funciton that scans for new files and adds to fileshare
def copy_new_files(source, dest):
    counter = 0
   
    #push last scan back 2h to catch stragglers
    last_scan_time = last_run() - (2 * 60 * 60)
    for root, dirs, files in os.walk(source):
        #GET MODTIME
        dirs[:] = [ d for d in dirs if os.path.getmtime(os.path.join(root,d)) > last_scan_time]
        try:  
            relative_path = os.path.relpath(root, source)
            destination_dir = os.path.join(dest, relative_path)
            
            for file in files:
                if os.path.getmtime(file) > last_scan_time:
                    source_file = os.path.join(root, file)
                    dest_file = os.path.join(destination_dir,file)
                    # if newer, copy file. or if it doesnt exist in the destination
                    if os.path.getmtime(source_file) > last_scan_time and  not os.path.exists(dest_file):
                        print(f"Moving {file}")
                        client.fput_object(
                            bucket_name,
                            dest_file,
                            source_file)
                        counter += 1
                        print(f"{source_file} copied to {dest_file}")
                        logging.info(f"{source_file} copied to {dest_file}")
                    else:
                        continue
        except Exception as e:
            print(e)
            pass
    return counter
     

def main():
   counter = 0
   
   start_time = time.time()
   for source_folder, destination in zip(source_folders, destinations):
       print(f"Processing {source_folder}")
       counter += copy_new_files(source_folder,destination)
   end_time = time.time() 
   times = round((end_time - start_time),2)    
   if counter == 0:
       print(f"Nothing to copy. Total runtime: {times} seconds")
       logging.info(f"Nothing to copy. Total runtime: {times} seconds")
   else:   
       print(f"{counter} files copied. Total runtime: {times} seconds")
       logging.info(f"{counter} files copied. Total runtime: {times} seconds")
   
    #update last run
   with open(last_scan, 'w') as f:
       f.write(str(datetime.now().timestamp()))
   
    #ping healthchecks
   urllib.request.urlopen("https://hc-ping.com/e32f6368-9d1d-4427-8428-5b37a2505a78")
    
if __name__ == '__main__':   
    main()
