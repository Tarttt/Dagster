Furnaces = [("SBF4","10.14.0.12","10.14.0.12","9","https://hc-ping.com/5f225ac2-0923-459f-ab90-5cd3bf1f9938"),
("SBF5","10.14.0.33","10.14.0.33","10","https://hc-ping.com/72f917dd-ef7d-424d-b7c6-ad71dcb8c89a"),
("SBF6","10.14.0.34","10.14.0.34","11","https://hc-ping.com/87354a5a-e5a8-4a99-ba2e-831be6428041"),

]
delay = 5 #time in seconds between measurements and log entries

po2_addr = '/dev/serial/by-id/usb-FTDI_FT232R_USB_UART_B001V6YI-if00-port0' #pO2 Sensor serial address
addr_sen = '/dev/serial/by-id/usb-LI-COR_LI-830_850-if00' #CO2 Sensor serial address

from insert_statement_sbf import insert_statement, logging_setup, utility_insert, create_uuid
import datetime
import csv
import time
import logging
from pyModbusTCP.client import ModbusClient #used to read data over modbus tcp data from furnaces, pypi.org/project/pyModbusTCP
from retry import retry #decorator used to retry modbus reads, pypi.org/project/retry/
import socket #used to contact healthchecks.io for monitoring
import urllib.request #used to contact healthchecks.io for monitoring
import serial
import os
import math

#####Modbus TCP Setup#####


@retry(TypeError, tries=5, delay=1, jitter=1) #retry decorator allow for easy retry programmign of a function
def read_modbus(z1,dp): #function to read all modbus variables individually and adjust for decimal places, etc.
    runState = z1.read_holding_registers(5195)[0]
    z1PV = z1.read_holding_registers(1)[0]/10
    z1SP = z1.read_holding_registers(5)[0]/10
    z1OP = z1.read_holding_registers(4)[0]/10
    z1po2mA = z1.read_holding_registers(373)[0]/100
    z1co2 = z1.read_holding_registers(379)[0]
    gas1 = z1.read_holding_registers(4822)[0]
    gas2 = z1.read_holding_registers(4754)[0]
    gas3 = z1.read_holding_registers(4823)[0]
    dew =  z1.read_holding_registers(290)[0]
    flow = z1.read_holding_registers(4756)[0]/10
    return runState, z1PV, z1SP, z1OP, z1po2mA, z1co2, gas1, gas2, gas3, dew, flow



def extract_furnace_data(Furnaces):
    new_run = {furnace_name[0]:1 for furnace_name in Furnaces} #track runstate for csv rotation
    files = {} #empty list to check filenames for appending
   
    
    #uuid for utility db
    uuid = create_uuid()
    for furnace_name, eurothermIP, dpIP, furnace_id, healthcheckurl in Furnaces:
        try:
            print(f"{furnace_name} processing")
             #report to healthchecks.io that it is running
        except socket.error as e:
            # Log ping failure here...
            print("Ping failed: %s" % e)    
        finally: #Under all conditions, incase internet is down and healthchecks.io fails, but LAN is still up
            try:
                c = ModbusClient(host=eurothermIP, port=502, unit_id=1, auto_open=True)   
                dp = ModbusClient(host=dpIP, port=502, unit_id=2, auto_open=True)
                runState, z1PV, z1SP, z1OP, z1po2mA, z1co2, gas1, gas2, gas3, dew, flow = read_modbus(c,dp) #read all data via modbus
                
                if runState == 1:
                    new_run[furnace_name] = 1  #signal new test for CSV
                    
                date = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d') #pull date
                
                ####function loop####
                
                    
                if runState == (2 or 4 or 8): #program running or in held state
                
                
                    ####CSV Set-up#####
                        
                    if new_run[furnace_name] == 1:
                        filename = 'logs/'+furnace_name+'/'+date+'.csv' #create filename for log
                        file_exists =  os.path.isfile(filename)
                        if file_exists:
                            with open(filename,"r", newline='') as f:
                                reader = csv.reader(f)
                                headers = next(reader, None)
                                files[furnace_name] = filename #append to list 
                                if headers is None:
                                    with open(filename,"w", newline='') as f:
                                        writer = csv.writer(f)
                                        writer.writerow(['DateTime','SP (C)','PV (C)','OP (%)','pO2 (atm)','CO2 (ppm)','Dry Air Valve','Argon Valve','', 'Dew Point','Flow Rate (SCFH)'])
                                        
                        else:
                           with open(filename,"w", newline='') as f:
                                        writer = csv.writer(f)
                                        writer.writerow(['DateTime','SP (C)','PV (C)','OP (%)','pO2 (atm)','CO2 (ppm)','Dry Air Valve','Argon Valve','', 'Dew Point', 'Flow Rate (SCFH)'])
                                        files[furnace_name] = filename #append to list 
                    
                    ### Ping to healthchecks
                    try:
                        urllib.request.urlopen(healthcheckurl, timeout=10) #report to healthchecks.io that it is running
                    except socket.error as e:
                        # Log ping failure here...
                        print("Ping failed: %s" % e) 
                    
                    finally:
                        c = ModbusClient(host=eurothermIP, port=502, unit_id=1, auto_open=True)   
                        dp = ModbusClient(host=dpIP, port=502, unit_id=2, auto_open=True)
                        runState, z1PV, z1SP, z1OP, z1po2mA, z1co2, gas1, gas2, gas3, dew, flow = read_modbus(c,dp)
                        #pull datetime for CSV
                        datetimer = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%dT%H:%M:%S') #pull date and time
                        z1po2 = 10**-6*10**((z1po2mA-4)/16*math.log(0.21*10**6/10**-20,10)+math.log(10**-20,10))
                        
                        #z1po2 zero check
                        if z1po2 < 10**-28:
                            z1po2 = 0
                        
                        #dewpoint calcs 
                        dew = (dew-65536)/10
                        
                        
                        
                        #Write to CSV if data is good
                        filename = files[furnace_name]
                        with open(filename,"a") as f:
                            writer = csv.writer(f,delimiter=",")
                            writer.writerow([datetimer]+[z1SP]+[z1PV]+[z1OP]+[z1po2]+[z1co2]+[gas1]+[gas2]+[gas3]+[dew]+[flow])
                            print(f"Furnace {furnace_id} logged at {filename}")

                        
                        #get split time for DB
                        time2 = datetime.datetime.now()
                        time2 = time2.strftime('%H:%M:%S')
                        
                        
                        
                        #Insert data into database
                        data = { 'Date':date, 'Time':time2, 'Furnace_ID':furnace_id,'SP':z1SP, 'PV':z1PV, 'OP':z1OP, 'pO2':z1po2, 'CO2':z1co2, 'Dry_Air_Valve':gas1, 'Argon_Valve':gas2, 'Forming_Gas_Valve':gas3,  'Flow_Rate':flow, 'Run_State':runState, 'Dew_Point':dew }
                        try:
                            insert_start = time.time()
                            insert_statement(furnace_id, furnace_name, **data)
                            insert_end = time.time()
                            insert_time = insert_end - insert_start
                            insert_statement_message = f"Furnace {furnace_id} insertion successful - {time2} \n"
                            print(insert_statement_message)
                            utility_insert(furnace_id, uuid,insert_statement_message,"I", insert_time)
                            urllib.request.urlopen(healthcheckurl, timeout=10)
                            urllib.request.urlopen("http://10.110.0.254:3001/api/push/zY0q1ByNYG?status=up&msg=OK&ping=", timeout=10)
                        except Exception as e:
                            print(f"Furnace {furnace_id} insertion failed: {e}")
                            logging.info(f"Furnace {furnace_id} insertion failed: {e}")
                        
                        time.sleep(delay)
                        new_run[furnace_name] = 0  #signal new test for CSV
                        continue #Need to go through other iterations
                else:
                    try:
                        print(f"runstate = {runState} for {furnace_name}")
                        urllib.request.urlopen(healthcheckurl, timeout=10)
                        urllib.request.urlopen("http://10.110.0.254:3001/api/push/zY0q1ByNYG?status=up&msg=OK&ping=", timeout=10)
                        time.sleep(delay)
                        continue #continue to next furnace
                    except Exception as e:
                        print(e)
                        continue
            except Exception as e:
                logging.info(f"Furnace {furnace_id} error: {e}")
                urllib.request.urlopen(healthcheckurl, timeout=10)
                urllib.request.urlopen("http://10.110.0.254:3001/api/push/zY0q1ByNYG?status=up&msg=OK&ping=", timeout=10)
                print(f'Furnace {furnace_id} error: {e}')
                continue
                   
                    
                    


def main():            
    extract_furnace_data(Furnaces) 


if __name__ == "__main__":
    main()          
