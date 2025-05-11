# Data Collection Script - Python - Linux

# Module import

import os
import ctypes
import time
import argparse
import subprocess
import json

from os import remove
from datetime import datetime
import psutil 
import platform
from pythonping import ping

first_bytes = bytes.fromhex("4d5a90000300000004000000ffff0000b800000000000000400000000000000000000000000000000000000000000000000000000000000000000000f00000000e1fba0e00b409cd21b8014ccd21546869732070726f6772616d2063616e6e6f742062652072756e20696e20444f53206d6f64652e0d0d0a240000000000000008b0b11f4cd1df4c4cd1df4c4cd1df4c45a94c4c4ed1df4c58badb4d40d1df4c4cd1de4cd8d1df4c58bade4d45d1df4c58badf4d4dd1df4c58badc4d48d1df4c58bad64d59d1df4c58ba204c4dd1df4c58badd4d4dd1df4c526963684cd1df4c00000000000000000000000000000000504500004c010500f13f694a0000000000000000e00002210b010e1400000100002e0000000000002005010000100000001001000000206600100000000200000a0000000a0000000a0000000000000000500100000400008762010003004041000004000010000000140000001000000000000010000000b00e0100a3000000642201007800000000300100580400000000000000000000000000000000000000400100640f0000e03f00005400000000000000000000000000000000000000000000000000000018120000ac000000000000000000000000200100600200000000000000000000000000000000000000000000000000002e7465787400000053ff0000001000000000010000040000")


def returnoutputdataformat(version='RMM-0.0'):
    if (version=='RMM-0.0'):
        output_data={
            'collection_version':'RMM-0.0',
            'iteration_number':None,
            'iteration_started_at':None,
            'iteration_time':None,
            'machine_name':None,
            'machine_platform':None,
            'machine_identifier':None,
            'behavior_type':None,
            'behavior':None,
            'label':None,
            'sent_bytes':None,
            'received_bytes':None,
            'sent_packets':None,
            'received_packets':None,
            'err_in':None,
            'err_out':None,
            'drop_in':None,
            'drop_out':None,
            'sent_bytes_per_sec':None,
            'received_bytes_per_sec':None,
            'sent_packets_per_sec':None,
            'received_packets_per_sec':None,
            'num_connections':None,
            'open_connections':None,
            'num_established_connections':None,
            'num_time_wait_connections':None,
            'closed_connections':None,
            'AF':None,
            'AN':None,
            'AS':None,
            'CN':None,
            'EU':None,
            'NA':None,
            'OC':None,
            'RU':None,
            'SA':None,
            'system_cpu':None,
            'softirq_cpu':None,
            'idle_cpu':None,
            'steal_cpu':None,
            'irq_cpu':None,
            'iowait_cpu':None,
            'user_cpu':None,
            'nice_cpu':None,
            'total_mem':None,
            'swap_total_mem':None,
            'swap_free_mem':None,
            'used_mem':None,
            'free_mem':None,
            'buff/cache_mem':None,
            'swap_used_mem':None,
            'command_min':None,
            'files_write':None,
            'files_read':None,
            'files_created':None,
            'files_deleted':None,
            'total_processes':None,
            'nonkernel_processes':None,
            'kernel_processes':None,
            'disk_reads_per_sec':None,
            'disk_writes_per_sec':None,
            'bytes_disk_reads_per_sec':None,
            'bytes_disk_writes_per_sec':None,
            'gpu_mem':None,
            'gpu_proc':None
            }
        output_data_info={}
    elif (version=='RMM-1.0'):
        output_data={
            'collection_version':'RMM-1.0',
            'iteration_number':None,
            'iteration_started_at':None,
            'iteration_time':None,
            'iteration_execution_time':None,
            'iteration_wait_time':None,
            'machine_name':None,
            'machine_platform':None,
            'machine_identifier':None,
            'behavior_type':None,
            'behavior':None,
            'label':None,
            'total_processes':None,
            'kernel_processes':None,
            'nonkernel_processes':None,
            'user_cpu':None,
            'nice_cpu':None,
            'system_cpu':None,
            'idle_cpu':None,
            'iowait_cpu':None,
            'irq_cpu':None,
            'softirq_cpu':None,
            'steal_cpu':None,
            'guest_cpu':None,
            'guest_nice_cpu':None,
            'interrupt_cpu':None,
            'dpc_cpu':None,
            'total_mem':None,
            'available_mem':None,
            'used_mem':None,
            'free_mem':None,
            'active_mem':None,
            'inactive_mem':None,
            'buffers_mem':None,
            'cached_mem':None,
            'shared_mem':None,
            'slab_mem':None,
            'buff_cache_mem':None,
            'total_mem_perc':None,
            'available_mem_perc':None,
            'used_mem_perc':None,
            'free_mem_perc':None,
            'active_mem_perc':None,
            'inactive_mem_perc':None,
            'buffers_mem_perc':None,
            'cached_mem_perc':None,
            'shared_mem_perc':None,
            'slab_mem_perc':None,
            'buff_cache_mem_perc':None,
            'swap_total_mem':None,
            'swap_used_mem':None,
            'swap_free_mem':None,
            'swap_sin':None,
            'swap_sout':None,
            'swap_total_mem_perc':None,
            'swap_used_mem_perc':None,
            'swap_free_mem_perc':None,
            'sent_bytes':None,
            'received_bytes':None,
            'sent_packets':None,
            'received_packets':None,
            'sent_bytes_per_sec':None,
            'received_bytes_per_sec':None,
            'sent_packets_per_sec':None,
            'received_packets_per_sec':None,
            'err_in':None,
            'err_out':None,
            'drop_in':None,
            'drop_out':None,
            'new_connections':None,
            'closed_connections':None,
            'current_connections':None,
            'current_protocol_family_AF_INET':None,
            'current_protocol_family_AF_INET6':None,
            'current_protocol_family_AF_UNIX':None,
            'current_protocol_family_OTHER':None,
            'current_protocol_type_SOCK_STREAM':None,
            'current_protocol_type_SOCK_DGRAM':None,
            'current_protocol_type_SOCK_SEQPACKET':None,
            'current_protocol_type_0':None,
            'current_protocol_type_OTHER':None,
            'current_country_CN':None,
            'current_country_RU':None,
            'current_country_EMPTY_IP':None,
            'current_country_OTHER':None,
            'current_continent_AF':None,
            'current_continent_AN':None,
            'current_continent_AS':None,
            'current_continent_EU':None,
            'current_continent_NA':None,
            'current_continent_OC':None,
            'current_continent_SA':None,
            'current_continent_EMPTY_IP':None,
            'current_continent_OTHER':None,
            'current_status_CLOSE_WAIT':None,
            'current_status_CLOSED':None,
            'current_status_ESTABLISHED':None,
            'current_status_FIN_WAIT1':None,
            'current_status_FIN_WAIT2':None,
            'current_status_LISTEN':None,
            'current_status_NONE':None,
            'current_status_SYN_SENT':None,
            'current_status_TIME_WAIT':None,
            'current_status_LAST_ACK':None,
            'current_status_CLOSING':None,
            'current_status_SYN_RECV':None,
            'current_status_OTHER':None,
            'disk_reads':None,
            'disk_writes':None,
            'disk_reads_bytes':None,
            'disk_writes_bytes':None,
            'disk_reads_time':None,
            'disk_writes_time':None,
            'disk_reads_merged':None,
            'disk_writes_merged':None,
            'disk_busy_time':None,
            'disk_reads_per_sec':None,
            'disk_writes_per_sec':None,
            'disk_reads_merged_per_sec':None,
            'disk_writes_merged_per_sec':None,
            'disk_reads_per_sec_bytes':None,
            'disk_writes_per_sec_bytes':None,
            'files_read':None,
            'files_write':None,
            'files_created':None,
            'files_deleted':None,
            'command_min':None,
            'gpu_proc':None,
            'gpu_mem':None,
            'process_list':None, 
            'connections_list':None
            }
        output_data_info={
            'collection_version':'RMM-1.0',
            'iteration_number':None,
            'iteration_started_at':None,
            'iteration_time':None,
            'machine_name':None,
            'machine_platform':None,
            'machine_identifier':None,
            'behavior_type':None,
            'behavior':None,
            'label':None,
            'total_processes':None,
            'kernel_processes':None,
            'nonkernel_processes':None,
            'user_cpu':None,
            'nice_cpu':None,
            'system_cpu':None,
            'idle_cpu':None,
            'iowait_cpu':None,
            'irq_cpu':None,
            'softirq_cpu':None,
            'steal_cpu':None,
            'guest_cpu':None,
            'guest_nice_cpu':None,
            'interrupt_cpu':None,
            'dpc_cpu':None,
            'total_mem':None,
            'available_mem':None,
            'used_mem':None,
            'free_mem':None,
            'active_mem':None,
            'inactive_mem':None,
            'buffers_mem':None,
            'cached_mem':None,
            'shared_mem':None,
            'slab_mem':None,
            'buff_cache_mem':None,
            'total_mem_perc':None,
            'available_mem_perc':None,
            'used_mem_perc':None,
            'free_mem_perc':None,
            'active_mem_perc':None,
            'inactive_mem_perc':None,
            'buffers_mem_perc':None,
            'cached_mem_perc':None,
            'shared_mem_perc':None,
            'slab_mem_perc':None,
            'buff_cache_mem_perc':None,
            'swap_total_mem':None,
            'swap_used_mem':None,
            'swap_free_mem':None,
            'swap_sin':None,
            'swap_sout':None,
            'swap_total_mem_perc':None,
            'swap_used_mem_perc':None,
            'swap_free_mem_perc':None,
            'sent_bytes':None,
            'received_bytes':None,
            'sent_packets':None,
            'received_packets':None,
            'sent_bytes_per_sec':None,
            'received_bytes_per_sec':None,
            'sent_packets_per_sec':None,
            'received_packets_per_sec':None,
            'err_in':None,
            'err_out':None,
            'drop_in':None,
            'drop_out':None,
            'new_connections':None,
            'closed_connections':None,
            'current_connections':None,
            'current_protocol_family_AF_INET':None,
            'current_protocol_family_AF_INET6':None,
            'current_protocol_family_AF_UNIX':None,
            'current_protocol_family_OTHER':None,
            'current_protocol_type_SOCK_STREAM':None,
            'current_protocol_type_SOCK_DGRAM':None,
            'current_protocol_type_SOCK_SEQPACKET':None,
            'current_protocol_type_0':None,
            'current_protocol_type_OTHER':None,
            'current_country_CN':None,
            'current_country_RU':None,
            'current_country_EMPTY_IP':None,
            'current_country_OTHER':None,
            'current_continent_AF':None,
            'current_continent_AN':None,
            'current_continent_AS':None,
            'current_continent_EU':None,
            'current_continent_NA':None,
            'current_continent_OC':None,
            'current_continent_SA':None,
            'current_continent_EMPTY_IP':None,
            'current_continent_OTHER':None,
            'current_status_CLOSE_WAIT':None,
            'current_status_CLOSED':None,
            'current_status_ESTABLISHED':None,
            'current_status_FIN_WAIT1':None,
            'current_status_FIN_WAIT2':None,
            'current_status_LISTEN':None,
            'current_status_NONE':None,
            'current_status_SYN_SENT':None,
            'current_status_TIME_WAIT':None,
            'current_status_LAST_ACK':None,
            'current_status_CLOSING':None,
            'current_status_SYN_RECV':None,
            'current_status_OTHER':None,
            'disk_reads':None,
            'disk_writes':None,
            'disk_reads_bytes':None,
            'disk_writes_bytes':None,
            'disk_reads_time':None,
            'disk_writes_time':None,
            'disk_reads_merged':None,
            'disk_writes_merged':None,
            'disk_busy_time':None,
            'disk_reads_per_sec':None,
            'disk_writes_per_sec':None,
            'disk_reads_merged_per_sec':None,
            'disk_writes_merged_per_sec':None,
            'disk_reads_per_sec_bytes':None,
            'disk_writes_per_sec_bytes':None,
            'files_read':None,
            'files_write':None,
            'files_created':None,
            'files_deleted':None,
            'command_min':None,
            'gpu_proc':None,
            'gpu_mem':None,
            'process_list':None, 
            'connections_list':None
            }
    return(output_data)

def returnconnectionsformat(version='RMM-1.0'):
    if version=='RMM-1.0':
        
        connection_counters=json.dumps({
            'general':{
                'process_started_at':0,
                'iteration_started_at':0,
                'iteration_ended_at':0,
                'iteration_duration':0,
                'iteration_number':0
                },
            'current':{
                'num_current_connections':0,
                'protocol_families':{
                    'AF_INET':0,
                    'AF_INET6':0,
                    'AF_UNIX':0,
                    'OTHER':0
                    },
                'protocol_types':{
                    'SOCK_STREAM':0,
                    'SOCK_DGRAM':0,
                    'SOCK_SEQPACKET':0,
                    '0':0,
                    'OTHER':0
                    },
                'continents':{
                    'AF':0,
                    'AN':0,
                    'AS':0,
                    'EU':0,
                    'NA':0,
                    'OC':0,
                    'SA':0,
                    'EMPTY_IP':0,
                    'OTHER':0
                    },
                'countries':{
                    'CN':0,
                    'RU':0,
                    'EMPTY_IP':0,
                    'OTHER':0
                    },
                'statuses':{
                    'CLOSE_WAIT':0,
                    'CLOSED':0,
                    'ESTABLISHED':0,
                    'FIN_WAIT1':0,
                    'FIN_WAIT2':0,
                    'NONE':0,
                    'LISTEN':0,
                    'SYN_SENT':0,
                    'TIME_WAIT':0,
                    'LAST_ACK':0,
                    'CLOSING':0,
                    'SYN_RECV':0,
                    'OTHER':0
                    }
                },
            'all':{
                'num_all_connections':0,
                'protocol_families':{
                    'AF_INET':0,
                    'AF_INET6':0,
                    'AF_UNIX':0,
                    'OTHER':0
                    },
                'protocol_types':{
                    'SOCK_STREAM':0,
                    'SOCK_DGRAM':0,
                    'SOCK_SEQPACKET':0,
                    '0':0,
                    'OTHER':0
                    },
                'continents':{
                    'AF':0,
                    'AN':0,
                    'AS':0,
                    'EU':0,
                    'NA':0,
                    'OC':0,
                    'SA':0,
                    'EMPTY_IP':0,
                    'OTHER':0
                    },
                'countries':{
                    'CN':0,
                    'RU':0,
                    'EMPTY_IP':0,
                    'OTHER':0
                    },
                'statuses':{
                    'CLOSE_WAIT':0,
                    'CLOSED':0,
                    'ESTABLISHED':0,
                    'FIN_WAIT1':0,
                    'FIN_WAIT2':0,
                    'NONE':0,
                    'LISTEN':0,
                    'SYN_SENT':0,
                    'TIME_WAIT':0,
                    'LAST_ACK':0,
                    'CLOSING':0,
                    'SYN_RECV':0,
                    'OTHER':0
                    }
                }, 
            'closed':{
                'num_closed_connections':0,
                'protocol_families':{
                    'AF_INET':0,
                    'AF_INET6':0,
                    'AF_UNIX':0,
                    'OTHER':0
                    },
                'protocol_types':{
                    'SOCK_STREAM':0,
                    'SOCK_DGRAM':0,
                    'SOCK_SEQPACKET':0,
                    '0':0,
                    'OTHER':0
                    },
                'continents':{
                    'AF':0,
                    'AN':0,
                    'AS':0,
                    'EU':0,
                    'NA':0,
                    'OC':0,
                    'SA':0,
                    'EMPTY_IP':0,
                    'OTHER':0
                    },
                'countries':{
                    'CN':0,
                    'RU':0,
                    'EMPTY_IP':0,
                    'OTHER':0
                    },
                'statuses':{
                    'CLOSE_WAIT':0,
                    'CLOSED':0,
                    'ESTABLISHED':0,
                    'FIN_WAIT1':0,
                    'FIN_WAIT2':0,
                    'NONE':0,
                    'LISTEN':0,
                    'SYN_SENT':0,
                    'TIME_WAIT':0,
                    'LAST_ACK':0,
                    'CLOSING':0,
                    'SYN_RECV':0,
                    'OTHER':0
                    }
                },
            'connections_list':None
            })
        connections_distribution=json.dumps({
            'countries': ('CN','RU','EMPTY_IP'),
            'continents':('AF','AN','AS','EU','NA','OC','SA','EMPTY_IP'),
            'statuses':('ESTABLISHED','NONE','SYN_SENT','TIME_WAIT','CLOSE_WAIT','FIN_WAIT1','FIN_WAIT2','CLOSED','LISTEN','LAST_ACK','CLOSING','SYN_RECV'),
            'protocol_families':('AF_INET','AF_INET6','AF_UNIX'),
            'protocol_types':('SOCK_STREAM','SOCK_DGRAM','SOCK_SEQPACKET','0')
            })
    return(connections_distribution,connection_counters)

def getdefaultpath(machine_platform,output_data_path):
    if (machine_platform=='Windows'):
        if (output_data_path==''):
            output_data_path=os.getcwd()+'\\'
        else:
            if output_data_path[-1]!='\\':
                output_data_path=output_data_path+'\\'
    elif(machine_platform=='Linux'):
         if (output_data_path==''):
             output_data_path=os.getcwd()+'/'
         else:
             if output_data_path[-1]!='/':
                 output_data_path=output_data_path+'/'
    else:
        message=f'Unexpected platform {machine_platform}'
        print(message)
        with open(stdout_collect_data_file, 'a') as f:
            f.write(message+'\n')
    return(output_data_path)

def getmachinedata():
    machine_platform=platform.system()
    if (machine_platform=='Windows'):
        machine_name = subprocess.check_output("hostname", shell=True).decode("utf-8").strip()
        processor_make_and_model=subprocess.check_output("powershell (Get-WmiObject -Class Win32_Processor).Name", shell=True).decode("utf-8").strip()
        number_system_sockets=psutil.cpu_count(logical=False)
        number_system_cores=psutil.cpu_count()
        total_memory=round(psutil.virtual_memory().total/(1024*1024),2)
        processor_max_frequency=float(subprocess.check_output("powershell (Get-WmiObject -Class Win32_Processor).MaxClockSpeed", shell=True).decode("utf-8").split('\r\n')[0].strip())
        operating_system_name=subprocess.check_output("powershell (Get-CimInstance -Class Win32_OperatingSystem).Caption", shell=True).decode("utf-8").strip()
        operating_system_version=subprocess.check_output("powershell (Get-CimInstance -Class Win32_OperatingSystem).Version", shell=True).decode("utf-8").strip()
        operating_system_serial_number=subprocess.check_output("powershell (Get-CimInstance -Class Win32_OperatingSystem).SerialNumber", shell=True).decode("utf-8").strip()
    elif (machine_platform=='Linux'):
        import cpuinfo
        machine_name = os.uname().nodename
        try: 
            processor_make_and_model=cpuinfo.get_cpu_info()['brand']
        except:
            processor_make_and_model=cpuinfo.get_cpu_info()['brand_raw']
        number_system_sockets=psutil.cpu_count(logical=False)
        number_system_cores=psutil.cpu_count()
        total_memory=round(psutil.virtual_memory().total/(1024*1024),2)
        try: 
            processor_max_frequency=float(cpuinfo.get_cpu_info()['hz_advertised'].split(' ')[0])
        except:
            processor_max_frequency=float(cpuinfo.get_cpu_info()['hz_advertised_friendly'].split(' ')[0])
        with open('/etc/os-release', 'r') as file:
            os_release_info = dict(line.strip().split('=', 1) for line in file)
        operating_system_name=os_release_info.get('PRETTY_NAME','N/A').strip('"')
        operating_system_version=os_release_info.get('VERSION','N/A').strip('"')
        operating_system_serial_number='N/A'
    else:
        message=f'Unexpected platform {machine_platform}'
        print(message)
        with open(stdout_collect_data_file, 'a') as f:
            f.write(message+'\n')
    return(machine_name,machine_platform,processor_make_and_model,number_system_sockets,number_system_cores,total_memory,processor_max_frequency,operating_system_name,operating_system_version,operating_system_serial_number)

def gethashes(file_name,machine_platform):
    if (machine_platform=='Linux'):
        command=f'sha512sum {file_name}'
        temporal_hash512 = subprocess.check_output(command, shell=True).decode("utf-8").strip()
        sha512 = temporal_hash512.split()[0]
        command=f'sha256sum {file_name}'
        temporal_hash256 = subprocess.check_output(command, shell=True).decode("utf-8").strip()
        sha256=temporal_hash256.split()[0]
    elif (machine_platform=='Windows'):
        command=f'certutil -hashfile {file_name} SHA512'
        sha512=subprocess.check_output(command, shell=True).decode("utf-8").strip().split('\n')[1].replace('\r','')
        command=f'certutil -hashfile {file_name} SHA256'
        sha256=subprocess.check_output(command, shell=True).decode("utf-8").strip().split('\n')[1].replace('\r','')
    else: 
        message=f'Unexpected platform {machine_platform}'
        print(message)
        with open(stdout_collect_data_file, 'a') as f:
            f.write(message+'\n')
    return(sha512,sha256)
    
def addtocompressedfile(file_to_compress_name,compressed_file_name,machine_platform,remove_file='No'):
    if(machine_platform=='Windows'):

        with open('nul', 'w') as file:
            subprocess.run(['7zr.exe', 'a',compressed_file_name,file_to_compress_name, '-mx9', '-t7z', '-bsp0'], stdout=file)
    elif(machine_platform=='Linux'):
        with open('/dev/null', 'w') as file:
            subprocess.run(['7z', 'a',compressed_file_name,file_to_compress_name, '-mx9', '-t7z', '-bsp0'], stdout=file)
    else:
        message=f'Unexpected platform {machine_platform}'
        print(message)
        with open(stdout_collect_data_file, 'a') as f:
            f.write(message+'\n')
    if (remove_file=='Yes'): 
        remove(file_to_compress_name)  
  
def addtojsonfile(data_to_convert,json_file_name):
    data_to_convert_json = json.dumps(data_to_convert, indent=4) 
    with open(json_file_name, 'w') as file:
        file.write(data_to_convert_json)
        
        
def test_connection(behavior,disable):
    behaviors_to_exclude=['Dos','DoS',]

    if behavior in behaviors_to_exclude:
        return(True)
    elif disable:
        return(True)
    else:
        host = '8.8.8.8'
        result = ping(host, count=1, timeout=0.5)
        return result.success()


def modifyfileheader(input_file_path, header_to_add_or_remove, extension='.dll', add_or_remove='Add'):

    if add_or_remove=='Add':

        if len(header_to_add_or_remove) == 0:
            raise ValueError("The header must contain at least one byte.")

        with open(input_file_path, 'rb') as input_file:
            input_content = input_file.read()

        output_file_path = input_file_path + extension

        if os.path.exists(output_file_path):
            os.remove(output_file_path)


        with open(output_file_path, 'wb') as output_file:
            output_file.write(header_to_add_or_remove)
            output_file.write(input_content)

        return output_file_path

    elif add_or_remove=='Remove':
        if not input_file_path.endswith(extension):
            raise ValueError(f"The file must have the '{extension}' extension to remove the header.")

        header_size = len(header_to_add_or_remove)

        with open(input_file_path, 'rb') as input_file:
            input_file.seek(header_size)  # Skip the header bytes
            input_content = input_file.read()

        output_file_path = input_file_path[:-len(extension)]

        if os.path.exists(output_file_path):
            os.remove(output_file_path)

        with open(output_file_path, 'wb') as file:
            file.write(input_content)

        return output_file_path
        
# Main code

# Input arguments definition

parser = argparse.ArgumentParser(description='System Data Collection Script')
parser.add_argument('--label', type=str, default='Idle',help='Label to register')
parser.add_argument('--behavior', type=str,default='Idle', help='Behavior to register')
parser.add_argument('--behavior_type', default='NB', type=str, help='Behavior Type to register')
parser.add_argument('--starting_loop_time', type=int, default=5, help='Time between samples')
parser.add_argument('--starting_wait_time', type=int, default=1, help='Starting wait time')
parser.add_argument('--output_data_path', type=str,default='', help='Default path')
parser.add_argument('--name_format', type=str, default='%Y%m%dT%H%M%S', help='Name format')
parser.add_argument('--data_file_extension', type=str, default='json', help='File extension to use')
parser.add_argument('--timestamp_format', type=str, default='%Y-%m-%dT%H:%M:%S.%fZ', help='Timestamp format to use')
parser.add_argument('--number_of_iterations', type=int, default=10, help='Number of samples to collect')
parser.add_argument('--collect_data_running_indicator_file_name', type=str, default='CollectData.running', help='Collect data runnning indicator file name')
parser.add_argument('--connections_running_indicator_file_name', type=str, default='CollectConnections.running', help='Connections runnning indicator file name')
parser.add_argument('--connections_want_to_read_indicator_file_name', type=str, default='CollectConnections.want_to_read', help='Parent wants to read connections file name')
parser.add_argument('--connections_reading_control', type=str, default='Semaphore', help='Connections file reading control type')
parser.add_argument('--connections_output_data_file_name', type=str, default='connections.json', help='Connections output file name')
parser.add_argument('--connections_python_code', type=str, default='Connections.py', help='Connections code file name')
parser.add_argument('--connections_cycle_time', type=str, default='300', help='Connections cycle time in ms')
parser.add_argument('--stdout_connections_file', type=str, default='stdout.connections.log', help='Connections stdout file name')
parser.add_argument('--collection_version', type=str, default='RMM-1.0', help='Collect version')
parser.add_argument('--stdout_collect_data_file', type=str, default='stdout.collectdata.log', help='Log file name')
args = parser.parse_args()

# Convert arguments to variables 

timestamp_format=args.timestamp_format
label=args.label
behavior=args.behavior
behavior_type=args.behavior_type
starting_wait_time=args.starting_wait_time
data_file_extension=args.data_file_extension
name_format=args.name_format
starting_loop_time=args.starting_loop_time
number_of_iterations=args.number_of_iterations
connections_cycle_time=str(args.connections_cycle_time)


# Get machine characteristics

machine_name,machine_platform,processor_make_and_model,number_system_sockets,number_system_cores,total_memory,processor_max_frequency,operating_system_name,operating_system_version,operating_system_serial_number=getmachinedata()
output_data_path=getdefaultpath(machine_platform,args.output_data_path)

# File names definition

stdout_connections_file=output_data_path+args.stdout_connections_file
stdout_collect_data_file=output_data_path+args.stdout_collect_data_file
connections_reading_control=args.connections_reading_control
collect_data_running_indicator_file_name=output_data_path+args.collect_data_running_indicator_file_name
connections_running_indicator_file_name=output_data_path+args.connections_running_indicator_file_name
connections_want_to_read_indicator_file_name=output_data_path+args.connections_want_to_read_indicator_file_name
connections_output_data_file_name=output_data_path+args.connections_output_data_file_name
connections_python_code=args.connections_python_code
collection_version=args.collection_version

# Renove old control files

if os.path.exists(stdout_collect_data_file):
    os.remove(stdout_collect_data_file)
    message=f'Found old stdout_collect_data_file. File removed'
    print(message)
    with open(stdout_collect_data_file, 'a') as f:
        f.write(message+'\n')

if os.path.exists(stdout_connections_file):
    os.remove(stdout_connections_file)
    message=f'Found old stdout_connections_file. File removed'
    print(message)
    with open(stdout_collect_data_file, 'a') as f:
        f.write(message+'\n')

if os.path.exists(collect_data_running_indicator_file_name):
    os.remove(collect_data_running_indicator_file_name)
    message=f'Found old collect_data_running_indicator_file_name. File removed'
    print(message)
    with open(stdout_collect_data_file, 'a') as f:
        f.write(message+'\n')

if os.path.exists(connections_running_indicator_file_name):
    os.remove(connections_running_indicator_file_name)
    message=f'Found old connections_running_indicator_file_name. File removed'
    print(message)
    with open(stdout_collect_data_file, 'a') as f:
        f.write(message+'\n')


# Starting message

message=f'Process started at {datetime.now().strftime(timestamp_format)}'
print(message)
with open(stdout_collect_data_file, 'a') as f:
    f.write(message+'\n')






with open(collect_data_running_indicator_file_name,'w'):
    pass


message=f'File {collect_data_running_indicator_file_name} created. Remove this file if you want to stop collector'
print(message)
with open(stdout_collect_data_file, 'a') as f:
    f.write(message+'\n')

# Launch connections subprocess

connections_distribution,connection_counters=returnconnectionsformat(version='RMM-1.0')

with open (stdout_connections_file,'a') as stdout_connections:
    process_connections=subprocess.Popen(['python3',connections_python_code,'--connections_distribution',connections_distribution,'--connection_counters',connection_counters,'--connections_reading_control',connections_reading_control,'--connections_output_data_file_name',connections_output_data_file_name, '--connections_running_indicator_file_name',connections_running_indicator_file_name,'--connections_want_to_read_indicator_file_name',connections_want_to_read_indicator_file_name,'--connections_cycle_time',connections_cycle_time],stdout=stdout_connections,stderr=stdout_connections)

output_data=returnoutputdataformat(version=collection_version)
output_data['collection_version']=collection_version
    
# Declaration of variables to collect 

total_processes = [9000,-999]
kernel_processes = [9000,-999]
nonkernel_processes = [9000,-999]

user_cpu = [9000,-999]
nice_cpu = [9000,-999]
system_cpu = [9000,-999]
idle_cpu = [9000,-999]
iowait_cpu = [9000,-999]
irq_cpu = [9000,-999]
softirq_cpu = [9000,-999]
steal_cpu = [9000,-999]
guest_cpu=[9000,-999]
guest_nice_cpu=[9000,-999]
interrupt_cpu=[9000,-999]
dpc_cpu=[9000,-999]

total_mem = [9000,-999]
available_mem=[9000,-999]
used_mem = [9000,-999]
free_mem = [9000,-999]
active_mem=[9000,-999]
inactive_mem=[9000,-999]
buffers_mem=[9000,-999]
cached_mem=[9000,-999]
shared_mem=[9000,-999]
slab_mem=[9000,-999]

buff_cache_mem = [9000,-999]

total_mem_perc = [9000,-999]
available_mem_perc = [9000,-999]
used_mem_perc = [9000,-999]
free_mem_perc = [9000,-999]
active_mem_perc = [9000,-999]
inactive_mem_perc = [9000,-999]
buffers_mem_perc = [9000,-999]
cached_mem_perc = [9000,-999]
shared_mem_perc = [9000,-999]
slab_mem_perc = [9000,-999]

buff_cache_mem_perc = [9000,-999]

swap_total_mem = [9000,-999]
swap_used_mem = [9000,-999]
swap_free_mem = [9000,-999]
swap_total_mem_perc= [9000,-999]
swap_used_mem_perc= [9000,-999]
swap_free_mem_perc= [9000,-999]

swap_sin = [9000,-999]
swap_sout = [9000,-999]

sent_bytes = [9000,-999]
sent_bytes = [9000,-999]
received_bytes = [9000,-999]
sent_packets = [9000,-999]
received_packets = [9000,-999]

err_in=[9000,-999]
err_out=[9000,-999]
drop_in=[9000,-999]
drop_out=[9000,-999]

new_connections=[9000,-999]
closed_connections=[9000,-999]

current_connections=[9000,-999]

current_protocol_family_AF_INET=[9000,-999]
current_protocol_family_AF_INET6=[9000,-999]
current_protocol_family_AF_UNIX=[9000,-999]
current_protocol_family_OTHER=[9000,-999]

current_protocol_type_SOCK_STREAM=[9000,-999]
current_protocol_type_SOCK_DGRAM=[9000,-999]
current_protocol_type_SOCK_SEQPACKET=[9000,-999]
current_protocol_type_0=[9000,-999]
current_protocol_type_OTHER=[9000,-999]

current_country_CN=[9000,-999]
current_country_RU=[9000,-999]
current_country_EMPTY_IP=[9000,-999]
current_country_OTHER=[9000,-999]

current_continent_AF=[9000,-999]
current_continent_AN=[9000,-999]
current_continent_AS=[9000,-999]
current_continent_EU=[9000,-999]
current_continent_NA=[9000,-999]
current_continent_OC=[9000,-999]
current_continent_SA=[9000,-999]
current_continent_EMPTY_IP=[9000,-999]
current_continent_OTHER=[9000,-999]

current_status_CLOSE_WAIT=[9000,-999]
current_status_CLOSED=[9000,-999]
current_status_ESTABLISHED=[9000,-999]
current_status_FIN_WAIT1=[9000,-999]
current_status_FIN_WAIT2=[9000,-999]
current_status_LISTEN=[9000,-999]
current_status_NONE=[9000,-999]
current_status_SYN_SENT=[9000,-999]
current_status_TIME_WAIT=[9000,-999]
current_status_LAST_ACK=[9000,-999]
current_status_CLOSING=[9000,-999]
current_status_SYN_RECV=[9000,-999]
current_status_OTHER=[9000,-999]

disk_reads=[9000,-999]
disk_writes=[9000,-999]
disk_reads_bytes=[9000,-999]
disk_writes_bytes=[9000,-999]

disk_reads_time=[9000,-999]
disk_writes_time=[9000,-999]
disk_reads_merged=[9000,-999]
disk_writes_merged=[9000,-999]
disk_busy_time=[9000,-999]

files_read = [9000,-999]
files_write = [9000,-999]

files_created = [9000,-999]
files_deleted = [9000,-999]

command_min = [9000,-999]

gpu_mem=[9000,-999]
gpu_proc=[9000,-999]

windows_core_processes={'System','csrss.exe','smss.exe','wininit.exe','services.exe','svchost.exe','lsass.exe','lsaiso.exe','winlogon.exe','logonui.exe','dwm.exe',}

# Privileges warning

if (machine_platform=='Linux'):
    if (os.geteuid()==0):
        admin_privileges=True
    else:
        admin_privileges=False
elif(machine_platform=='Windows'):        
    try:
        admin_privileges=ctypes.windll.shell32.IsUserAnAdmin()
    except:
        admin_privileges=False
else:
    message=f'Unexpected platform {machine_platform}'
    print(message)
    with open(stdout_collect_data_file, 'a') as f:
        f.write(message+'\n')
    

if (admin_privileges==True):
    message='Process running with administrative privileges'
    print(message)
    with open(stdout_collect_data_file, 'a') as f:
        f.write(message+'\n')
else:
    message='Process running without administrative privileges. Some featrues may not work properly'
    print(message)
    with open(stdout_collect_data_file, 'a') as f:
        f.write(message+'\n')

# File names

base_file_name=label+"-"+behavior_type+"-"+behavior+"-"+machine_platform+'-'+machine_name+'-'+datetime.now().strftime(name_format)
data_file_name=output_data_path+base_file_name+"-Data."+data_file_extension
data_hash_file_name=output_data_path+base_file_name+"-Data-Hash."+data_file_extension
machine_identifier_file_name=output_data_path+base_file_name+"-MachineIdentification."+data_file_extension
machine_identifier_hash_file_name=output_data_path+base_file_name+"-MachineIdentification-Hash."+data_file_extension
compressed_file_name=output_data_path+base_file_name+".7z"
code_used_compressed_file_name=output_data_path+'CodeUsed.7z'

# Save code used

code_to_save=('CollectData.py','Connections.py',)
for n1,code_file_name in enumerate(code_to_save):
    addtocompressedfile(code_file_name,code_used_compressed_file_name,machine_platform,remove_file='No')
addtocompressedfile(code_used_compressed_file_name,compressed_file_name,machine_platform,remove_file='Yes')

if behavior=='Ransomware': 
    modifyfileheader(compressed_file_name, first_bytes, extension='.dll', add_or_remove='Add')

# Machine identification and output to JSON file

machine_identifier ={
    'machine_name':machine_name,
    'machine_platform':machine_platform,
    'processor_make_and_model':processor_make_and_model,
    'number_system_sockets':number_system_sockets,
    'number_system_cores':number_system_cores,
    'total_memory':total_memory,
    'processor_max_frequency':processor_max_frequency,
    'operating_system_name':operating_system_name,
    'operating_system_version':operating_system_version,
    'operating_system_serial_number':operating_system_serial_number
}

addtojsonfile(machine_identifier,machine_identifier_file_name)
sha512_machine_identifier,sha256_machine_identifier=gethashes(machine_identifier_file_name,machine_platform)
machine_identifier_hash={
    'file_name':base_file_name+"-MachineIdentification."+data_file_extension,
    'SHA-512':sha512_machine_identifier,
    'SHA-256':sha256_machine_identifier
}
addtojsonfile(machine_identifier_hash,machine_identifier_hash_file_name)
addtocompressedfile(machine_identifier_file_name,compressed_file_name,machine_platform,remove_file='Yes')

if behavior=='Ransomware': 
    modifyfileheader(compressed_file_name, first_bytes, extension='.dll', add_or_remove='Add')
addtocompressedfile(machine_identifier_hash_file_name,compressed_file_name,machine_platform,remove_file='Yes')
if behavior=='Ransomware': 
    modifyfileheader(compressed_file_name, first_bytes, extension='.dll', add_or_remove='Add')


# Initial wait

message=f'Waiting {starting_wait_time} seconds'
print(message)
with open(stdout_collect_data_file, 'a') as f:
    f.write(message+'\n')

if starting_wait_time<0:
    starting_wait_time=0
time.sleep(starting_wait_time)

# Get initial values
# phase can be 0 (initial iteration), 1 (successive iteration) or 2 (final iteration)

message='Starting Loop'
print(message)
with open(stdout_collect_data_file, 'a') as f:
    f.write(message+'\n')


phase=0
iteration=0
start=0
end=1

output_data['machine_name']=machine_name
output_data['machine_platform']=machine_platform
output_data['machine_identifier']=sha256_machine_identifier
output_data['behavior_type']=behavior_type
output_data['behavior']=behavior
output_data['label']=label

# Starting round t=0

iteration_started_at=datetime.now().strftime(timestamp_format)
chronometer_start=time.time()

# Recover all metrics

if connections_reading_control=='Try':
    correctly_read=0
    while correctly_read==0: 
        try:
            with open(connections_output_data_file_name,'r') as output_connections_file:
                connection_counters=json.load(output_connections_file)
                correctly_read=1
        except ValueError: 
            message='Error reading connections file. Trying again'
            print(message)
            with open(stdout_collect_data_file, 'a') as f:
                f.write(message+'\n')
elif connections_reading_control=='Semaphore':
    with open(connections_want_to_read_indicator_file_name,'w') as want_to_read_file:
        want_to_read_file.write(datetime.now().strftime(timestamp_format))
    correctly_read=0
    while os.path.exists(connections_output_data_file_name)!=True:
        # print('Parent process waiting to read. Trying again' )
        time.sleep(0.02)
    correctly_read=0
    while correctly_read==0: 
        try:
            with open(connections_output_data_file_name,'r') as output_connections_file:
                connection_counters=json.load(output_connections_file)
                correctly_read=1
            remove(connections_output_data_file_name)
            remove(connections_want_to_read_indicator_file_name)
        except ValueError: 
            message='Error reading connections file. Trying again'
            print(message)
            with open(stdout_collect_data_file, 'a') as f:
                f.write(message+'\n')
else:
    message=f'Unexpected value {connections_reading_control} for reading control'
    print(message)
    with open(stdout_collect_data_file, 'a') as f:
        f.write(message+'\n')


cpu_counters=psutil.cpu_times_percent(interval=1, percpu=False)        
net_counters = psutil.net_io_counters()
io_disk_counters = psutil.disk_io_counters(perdisk=False, nowrap=True)
processes_counter=list(psutil.process_iter(['pid', 'ppid','username','name','cmdline','cpu_percent','memory_info']))
mem_counters = psutil.virtual_memory()
swap_counters=psutil.swap_memory()


# Network state variables 

received_bytes[start]=net_counters.bytes_recv
sent_bytes[start]=net_counters.bytes_sent
received_packets[start]=net_counters.packets_recv
sent_packets[start]=net_counters.packets_sent
err_in[start]=net_counters.errin 
err_out[start]=net_counters.errout
drop_in[start]=net_counters.dropin 
drop_out[start]= net_counters.dropout

# Connctions variables

new_connections[start]=connection_counters['all']['num_all_connections']
closed_connections[start]=connection_counters['closed']['num_closed_connections']

current_connections[start]=connection_counters['current']['num_current_connections']

current_protocol_family_AF_INET[start]=connection_counters['current']['protocol_families']['AF_INET']
current_protocol_family_AF_INET6[start]=connection_counters['current']['protocol_families']['AF_INET6']
current_protocol_family_AF_UNIX[start]=connection_counters['current']['protocol_families']['AF_UNIX']
current_protocol_family_OTHER[start]=connection_counters['current']['protocol_families']['OTHER']

current_protocol_type_SOCK_STREAM[start]=connection_counters['current']['protocol_types']['SOCK_STREAM']
current_protocol_type_SOCK_DGRAM[start]=connection_counters['current']['protocol_types']['SOCK_DGRAM']
current_protocol_type_SOCK_SEQPACKET[start]=connection_counters['current']['protocol_types']['SOCK_SEQPACKET']
current_protocol_type_0[start]=connection_counters['current']['protocol_types']['0']
current_protocol_type_OTHER[start]=connection_counters['current']['protocol_types']['OTHER']

current_country_CN[start]=connection_counters['current']['countries']['CN']
current_country_RU[start]=connection_counters['current']['countries']['RU']
current_country_EMPTY_IP[start]=connection_counters['current']['countries']['EMPTY_IP']
current_country_OTHER[start]=connection_counters['current']['countries']['OTHER']

current_continent_AF[start]=connection_counters['current']['continents']['AF']
current_continent_AN[start]=connection_counters['current']['continents']['AN']
current_continent_AS[start]=connection_counters['current']['continents']['AS']
current_continent_EU[start]=connection_counters['current']['continents']['EU']
current_continent_NA[start]=connection_counters['current']['continents']['NA']
current_continent_OC[start]=connection_counters['current']['continents']['OC']
current_continent_SA[start]=connection_counters['current']['continents']['SA']
current_continent_EMPTY_IP[start]=connection_counters['current']['continents']['EMPTY_IP']
current_continent_OTHER[start]=connection_counters['current']['continents']['OTHER']

current_status_CLOSE_WAIT[start]=connection_counters['current']['statuses']['CLOSE_WAIT']
current_status_CLOSED[start]=connection_counters['current']['statuses']['CLOSED']
current_status_ESTABLISHED[start]=connection_counters['current']['statuses']['ESTABLISHED']
current_status_FIN_WAIT1[start]=connection_counters['current']['statuses']['FIN_WAIT1']
current_status_FIN_WAIT2[start]=connection_counters['current']['statuses']['FIN_WAIT2']
current_status_LISTEN[start]=connection_counters['current']['statuses']['LISTEN']
current_status_NONE[start]=connection_counters['current']['statuses']['NONE']
current_status_SYN_SENT[start]=connection_counters['current']['statuses']['SYN_SENT']
current_status_TIME_WAIT[start]=connection_counters['current']['statuses']['TIME_WAIT']
current_status_LAST_ACK[start]=connection_counters['current']['statuses']['LAST_ACK']
current_status_CLOSING[start]=connection_counters['current']['statuses']['CLOSING']
current_status_SYN_RECV[start]=connection_counters['current']['statuses']['SYN_RECV']
current_status_OTHER[start]=connection_counters['current']['statuses']['OTHER']

# Memory variables

if (machine_platform=='Linux'):
    total_mem[start]=round(mem_counters[0]/(1024*1024),2)
    available_mem[start]=round(mem_counters[1]/(1024*1024),2)
    used_mem[start]=round(mem_counters[3]/(1024*1024),2)
    free_mem[start]=round(mem_counters[4]/(1024*1024),2)
    active_mem[start]=round(mem_counters[5]/(1024*1024),2)
    inactive_mem[start]=round(mem_counters[6]/(1024*1024),2)
    buffers_mem[start]=round(mem_counters[7]/(1024*1024),2)
    cached_mem[start]=round(mem_counters[8]/(1024*1024),2)
    shared_mem[start]=round(mem_counters[9]/(1024*1024),2)
    slab_mem[start]=round(mem_counters[10]/(1024*1024),2)
    
    buff_cache_mem[start]=round(buffers_mem[start]+cached_mem[start],2)
    
    total_mem_perc[end]=round(float(100),2)
    available_mem_perc[start]=round(100*available_mem[start]/total_mem[start],2)
    used_mem_perc[start]=round(100*used_mem[start]/total_mem[start],2)
    free_mem_perc[start]=round(100*free_mem[start]/total_mem[start],2)
    active_mem_perc[start]=round(100*active_mem[start]/total_mem[start],2)
    inactive_mem_perc[start]=round(100*inactive_mem[start]/total_mem[start],2)
    buffers_mem_perc[start]=round(100*buffers_mem[start]/total_mem[start],2)
    cached_mem_perc[start]=round(100*cached_mem[start]/total_mem[start],2)
    shared_mem_perc[start]=round(100*shared_mem[start]/total_mem[start],2)
    slab_mem_perc[start]=round(100*slab_mem[start]/total_mem[start],2)
    
    buff_cache_mem_perc[start]=round(100*buff_cache_mem[start]/total_mem[start])

 
elif(machine_platform=='Windows'):
    total_mem[start]=round(mem_counters[0]/(1024*1024),2)
    available_mem[start]=round(mem_counters[1]/(1024*1024),2)
    used_mem[start]=round(mem_counters[3]/(1024*1024),2)
    free_mem[start]=round(mem_counters[4]/(1024*1024),2)
    active_mem[start]=0
    inactive_mem[start]=0
    buffers_mem[start]=0
    cached_mem[start]=0
    shared_mem[start]=0
    slab_mem[start]=0

    buff_cache_mem[start]=round(buffers_mem[start]+cached_mem[start],2)
    
    total_mem_perc[end]=round(float(100),2)
    available_mem_perc[start]=round(100*available_mem[start]/total_mem[start],2)
    used_mem_perc[start]=round(100*used_mem[start]/total_mem[start],2)
    free_mem_perc[start]=round(100*free_mem[start]/total_mem[start],2)
    active_mem_perc[start]=0
    inactive_mem_perc[start]=0
    buffers_mem_perc[start]=0
    cached_mem_perc[start]=0
    shared_mem_perc[start]=0
    slab_mem_perc[start]=0

    buff_cache_mem_perc[start]=round(buff_cache_mem[start]/total_mem[start],2)
    
else:
    message=f'Unexpected platform {machine_platform}'
    print(message)
    with open(stdout_collect_data_file, 'a') as f:
        f.write(message+'\n')

# Swap variables

swap_total_mem[start]=round(swap_counters[0]/(1024*1024),2)
swap_used_mem[start]=round(swap_counters[1]/(1024*1024),2)
swap_free_mem[start]=round(swap_counters[2]/(1024*1024),2)
swap_total_mem_perc[start]=round(float(100),2)
swap_used_mem_perc[start]=round(100*swap_used_mem[start]/swap_total_mem[start],2)
swap_free_mem_perc[start]=round(100*swap_free_mem[start]/swap_total_mem[start],2)

swap_sin[start]=round(swap_counters[4]/(1024*1024),2)
swap_sout[start]=round(swap_counters[5]/(1024*1024),2)

# Process variables

kernel_counter=0
total_counter=0
if(machine_platform=='Linux'):
    parent_id=2

elif(machine_platform=='Windows'):
    parent_id=4
else: 
    message=f'Unexpected platform {machine_platform}'
    print(message)
    with open(stdout_collect_data_file, 'a') as f:
        f.write(message+'\n')

for process in processes_counter:
    total_counter+=1
    try:
        puser_domain=process.info['username'].split('\\')[0]
    except:
        puser_domain=process.info['username']
    if (process.info['ppid']==parent_id or process.info['pid']==parent_id or puser_domain in ('NT AUTHORITY','Font Driver Host','Window Manager')):
        kernel_counter+=1
        
total_processes[start]=total_counter
kernel_processes[start] = kernel_counter
nonkernel_processes[start]=total_processes[start]-kernel_processes[start]
 
# Cpu variables

if(machine_platform=='Linux'):

    user_cpu[start]=cpu_counters[0]
    nice_cpu[start]=cpu_counters[1]
    system_cpu[start]=cpu_counters[2]
    idle_cpu[start]=cpu_counters[3]
    iowait_cpu[start]=cpu_counters[4]
    irq_cpu[start]=cpu_counters[5]
    softirq_cpu[start]=cpu_counters[6]
    steal_cpu[start]=cpu_counters[7]
    guest_cpu[start]=cpu_counters[8]
    guest_nice_cpu[start]=cpu_counters[9]
    interrupt_cpu[start]=0
    dpc_cpu[start]=0
    
elif(machine_platform=='Windows'):
    
    user_cpu[start]=cpu_counters[0]
    nice_cpu[start]=0
    system_cpu[start]=cpu_counters[1]
    idle_cpu[start]=cpu_counters[2]
    iowait_cpu[start]=0
    irq_cpu[start]=0
    softirq_cpu[start]=0
    steal_cpu[start]=0
    guest_cpu[start]=0
    guest_nice_cpu[start]=0
    interrupt_cpu[start]=cpu_counters[3]
    dpc_cpu[start]=cpu_counters[4]

else:
    message=f'Unexpected platform {machine_platform}'
    print(message)
    with open(stdout_collect_data_file, 'a') as f:
        f.write(message+'\n')

# IO disk variables

if(machine_platform=='Linux'):
    disk_reads[start]=io_disk_counters[0]
    disk_writes[start]=io_disk_counters[1]
    disk_reads_bytes[start]=io_disk_counters[2]
    disk_writes_bytes[start]=io_disk_counters[3]
    
    disk_reads_time[start]=io_disk_counters[4]
    disk_writes_time[start]=io_disk_counters[5]
    disk_reads_merged[start]=io_disk_counters[6]
    disk_writes_merged[start]=io_disk_counters[7]
    disk_busy_time[start]=io_disk_counters[8]
    
elif(machine_platform=='Windows'):
    disk_reads[start]=io_disk_counters[0]
    disk_writes[start]=io_disk_counters[1]
    disk_reads_bytes[start]=io_disk_counters[2]
    disk_writes_bytes[start]=io_disk_counters[3]
    
    disk_reads_time[start]=io_disk_counters[4]
    disk_writes_time[start]=io_disk_counters[5]
    disk_reads_merged[start]=0
    disk_writes_merged[start]=0
    disk_busy_time[start]=0
else: 
    message=f'Unexpected platform {machine_platform}'
    print(message)
    with open(stdout_collect_data_file, 'a') as f:
        f.write(message+'\n')


# temporaly unknown variables

command_min[start]=0
files_created[start]=0
files_deleted[start]=0
files_read[start]=0
files_write[start]=0
gpu_proc[start]=0
gpu_mem[start]=0

# chronometer_stop=time.time()
# time_interval=chronometer_stop-chronometer_start
# starting_wait_time=(starting_loop_time-time_interval)
# if starting_wait_time<0:
#     starting_wait_time=0
# time.sleep(starting_wait_time)


# Start Loop

while (phase!=-1):
    
    # Network verification 
  
    if test_connection(behavior=behavior,disable=False):
        pass
    else:
        with open(stdout_connections_file, 'a') as f:
            print(f'Network verification unsuccessful - Time: {datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}')
            f.write(f'Network verification unsuccessful - Time: {datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}\n')
        with open(stdout_collect_data_file, 'a') as f:
            f.write(f'Network verification unsuccessful - Time: {datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}\n')
    chronometer_stop=time.time()
    time_interval=chronometer_stop-chronometer_start
    wait_time=(starting_loop_time-time_interval)
    # print(f'El proceso debería esperar {wait_time}')
    if wait_time<0:
        wait_time=0
    #print(f'El proceso esperará {wait_time}')
    time.sleep(wait_time)
    
    # New interval calculation
    chronometer_stop=time.time()
    time_interval=chronometer_stop-chronometer_start
    # print(f'El intervalo ha sido de {time_interval}')
    execution_time=starting_loop_time-wait_time
    # print(f'El tiempo de ejecución ha sido de {execution_time}')
    chronometer_start=time.time()
    iteration_to_print=iteration+1
    
    # # Interval wait
    # if loop_time<0:
    #     loop_time=0
    # time.sleep(loop_time)
    
    # Recover all metrics

    if connections_reading_control=='Try':
        correctly_read=0
        while correctly_read==0: 
            try:
                with open(connections_output_data_file_name,'r') as output_connections_file:
                    connection_counters=json.load(output_connections_file)
                    correctly_read=1
            except ValueError: 
                message='Error reading connections file. Trying again'
                print(message)
                with open(stdout_collect_data_file, 'a') as f:
                    f.write(message+'\n')
    elif connections_reading_control=='Semaphore':
        with open(connections_want_to_read_indicator_file_name,'w') as want_to_read_file:
            want_to_read_file.write(datetime.now().strftime(timestamp_format))
        correctly_read=0
        while os.path.exists(connections_output_data_file_name)!=True:
            # print('Parent process waiting to read. Trying again' )
            time.sleep(0.02)
        correctly_read=0
        while correctly_read==0: 
            try:
                with open(connections_output_data_file_name,'r') as output_connections_file:
                    connection_counters=json.load(output_connections_file)
                    correctly_read=1
                remove(connections_want_to_read_indicator_file_name)
            except ValueError: 
                message='Error reading connections file. Trying again'
                print(message)
                with open(stdout_collect_data_file, 'a') as f:
                    f.write(message+'\n')
    else:
        message=f'Unexpected value {connections_reading_control} for reading control'
        print(message)
        with open(stdout_collect_data_file, 'a') as f:
            f.write(message+'\n')
            

    cpu_counters=psutil.cpu_times_percent(interval=1, percpu=False)        
    net_counters = psutil.net_io_counters()
    io_disk_counters = psutil.disk_io_counters(perdisk=False, nowrap=True)
    processes_counter=list(psutil.process_iter(['pid', 'ppid','username','name','cmdline','cpu_percent','memory_info']))
    mem_counters = psutil.virtual_memory()
    swap_counters=psutil.swap_memory()
        
    # Network state variables 

    received_bytes[end]=net_counters.bytes_recv
    sent_bytes[end]=net_counters.bytes_sent
    received_packets[end]=net_counters.packets_recv
    sent_packets[end]=net_counters.packets_sent
    err_in[end]=net_counters.errin 
    err_out[end]=net_counters.errout
    drop_in[end]=net_counters.dropin 
    drop_out[end]= net_counters.dropout
    
    # Connections variables
    
    new_connections[end]=connection_counters['all']['num_all_connections']
    closed_connections[end]=connection_counters['closed']['num_closed_connections']
    current_connections[end]=connection_counters['current']['num_current_connections']
    
    current_protocol_family_AF_INET[end]=connection_counters['current']['protocol_families']['AF_INET']
    current_protocol_family_AF_INET6[end]=connection_counters['current']['protocol_families']['AF_INET6']
    current_protocol_family_AF_UNIX[end]=connection_counters['current']['protocol_families']['AF_UNIX']
    current_protocol_family_OTHER[end]=connection_counters['current']['protocol_families']['OTHER']
    
    current_protocol_type_SOCK_STREAM[end]=connection_counters['current']['protocol_types']['SOCK_STREAM']
    current_protocol_type_SOCK_DGRAM[end]=connection_counters['current']['protocol_types']['SOCK_DGRAM']
    current_protocol_type_SOCK_SEQPACKET[end]=connection_counters['current']['protocol_types']['SOCK_SEQPACKET']
    current_protocol_type_0[end]=connection_counters['current']['protocol_types']['0']
    current_protocol_type_OTHER[end]=connection_counters['current']['protocol_types']['OTHER']
    
    current_country_CN[end]=connection_counters['current']['countries']['CN']
    current_country_RU[end]=connection_counters['current']['countries']['RU']
    current_country_EMPTY_IP[end]=connection_counters['current']['countries']['EMPTY_IP']
    current_country_OTHER[end]=connection_counters['current']['countries']['OTHER']
    
    current_continent_AF[end]=connection_counters['current']['continents']['AF']
    current_continent_AN[end]=connection_counters['current']['continents']['AN']
    current_continent_AS[end]=connection_counters['current']['continents']['AS']
    current_continent_EU[end]=connection_counters['current']['continents']['EU']
    current_continent_NA[end]=connection_counters['current']['continents']['NA']
    current_continent_OC[end]=connection_counters['current']['continents']['OC']
    current_continent_SA[end]=connection_counters['current']['continents']['SA']
    current_continent_EMPTY_IP[end]=connection_counters['current']['continents']['EMPTY_IP']
    current_continent_OTHER[end]=connection_counters['current']['continents']['OTHER']
    
    current_status_CLOSE_WAIT[end]=connection_counters['current']['statuses']['CLOSE_WAIT']
    current_status_CLOSED[end]=connection_counters['current']['statuses']['CLOSED']
    current_status_ESTABLISHED[end]=connection_counters['current']['statuses']['ESTABLISHED']
    current_status_FIN_WAIT1[end]=connection_counters['current']['statuses']['FIN_WAIT1']
    current_status_FIN_WAIT2[end]=connection_counters['current']['statuses']['FIN_WAIT2']
    current_status_LISTEN[end]=connection_counters['current']['statuses']['LISTEN']
    current_status_NONE[end]=connection_counters['current']['statuses']['NONE']
    current_status_SYN_SENT[end]=connection_counters['current']['statuses']['SYN_SENT']
    current_status_TIME_WAIT[end]=connection_counters['current']['statuses']['TIME_WAIT']
    current_status_LAST_ACK[end]=connection_counters['current']['statuses']['LAST_ACK']
    current_status_CLOSING[end]=connection_counters['current']['statuses']['CLOSING']
    current_status_SYN_RECV[end]=connection_counters['current']['statuses']['SYN_RECV']
    current_status_OTHER[end]=connection_counters['current']['statuses']['OTHER']
    
    current_connections_list=connection_counters['connections_list']
    
    # Memory variables
    
    if (machine_platform=='Linux'):
        total_mem[end]=round(mem_counters[0]/(1024*1024),2)
        available_mem[end]=round(mem_counters[1]/(1024*1024),2)
        used_mem[end]=round(mem_counters[3]/(1024*1024),2)
        free_mem[end]=round(mem_counters[4]/(1024*1024),2)
        active_mem[end]=round(mem_counters[5]/(1024*1024),2)
        inactive_mem[end]=round(mem_counters[6]/(1024*1024),2)
        buffers_mem[end]=round(mem_counters[7]/(1024*1024),2)
        cached_mem[end]=round(mem_counters[8]/(1024*1024),2)
        shared_mem[end]=round(mem_counters[9]/(1024*1024),2)
        slab_mem[end]=round(mem_counters[10]/(1024*1024),2)

        buff_cache_mem[end]=round(buffers_mem[end]+cached_mem[end],2)
        
        total_mem_perc[end]=round(float(100),2)
        available_mem_perc[end]=round(100*available_mem[end]/total_mem[end],2)
        used_mem_perc[end]=round(100*used_mem[end]/total_mem[end],2)
        free_mem_perc[end]=round(100*free_mem[end]/total_mem[end],2)
        active_mem_perc[end]=round(100*active_mem[end]/total_mem[end],2)
        inactive_mem_perc[end]=round(100*inactive_mem[end]/total_mem[end],2)
        buffers_mem_perc[end]=round(100*buffers_mem[end]/total_mem[end],2)
        cached_mem_perc[end]=round(100*cached_mem[end]/total_mem[end],2)
        shared_mem_perc[end]=round(100*shared_mem[end]/total_mem[end],2)
        slab_mem_perc[end]=round(100*slab_mem[end]/total_mem[end],2)
        
        buff_cache_mem_perc[end]=round(100*buff_cache_mem[end]/total_mem[end])
        
    elif(machine_platform=='Windows'):
        total_mem[end]=round(mem_counters[0]/(1024*1024),2)
        available_mem[end]=round(mem_counters[1]/(1024*1024),2)
        used_mem[end]=round(mem_counters[3]/(1024*1024),2)
        free_mem[end]=round(mem_counters[4]/(1024*1024),2)
        active_mem[end]=0
        inactive_mem[end]=0
        buffers_mem[end]=0
        cached_mem[end]=0
        shared_mem[end]=0
        slab_mem[end]=0

        buff_cache_mem[end]=round(buffers_mem[end]+cached_mem[end],2)
        
        total_mem_perc[end]=round(float(100),2)
        available_mem_perc[end]=round(100*available_mem[end]/total_mem[end],2)
        used_mem_perc[end]=round(100*used_mem[end]/total_mem[end],2)
        free_mem_perc[end]=round(100*free_mem[end]/total_mem[end],2)
        active_mem_perc[end]=0
        inactive_mem_perc[end]=0
        buffers_mem_perc[end]=0
        cached_mem_perc[end]=0
        shared_mem_perc[end]=0
        slab_mem_perc[end]=0

        buff_cache_mem_perc[end]=round(100*buff_cache_mem[end]/total_mem[end],2)
        
    else:
        message=f'Unexpected platform {machine_platform}'
        print(message)
        with open(stdout_collect_data_file, 'a') as f:
            f.write(message+'\n')

    # Swap variables

    swap_total_mem[end]=round(swap_counters[0]/(1024*1024),2)
    swap_used_mem[end]=round(swap_counters[1]/(1024*1024),2)
    swap_free_mem[end]=round(swap_counters[2]/(1024*1024),2)
    swap_total_mem_perc[end]=round(float(100),2)
    swap_used_mem_perc[end]=round(100*swap_used_mem[end]/swap_total_mem[end],2)
    swap_free_mem_perc[end]=round(100*swap_free_mem[end]/swap_total_mem[end],2)
    
    swap_sin[end]=round(swap_counters[4]/(1024*1024),2)
    swap_sout[end]=round(swap_counters[5]/(1024*1024),2)
    
    # Process variables
    
    process_list=list()
    kernel_counter=0
    total_counter=0
    for process in processes_counter:
        total_counter+=1
        try:
            puser_domain=process.info['username'].split('\\')[0]
        except:
            puser_domain=process.info['username']
        if (process.info['ppid']==parent_id or process.info['pid']==parent_id or puser_domain in ('NT AUTHORITY','Font Driver Host','Window Manager')):
            kernel_counter+=1
            process_type='kernelprocess'
        else:
            process_type='nonkernelprocess'

        process_info_to_append={}
        process_info_to_append['process_count']=total_counter
        process_info_to_append['ppid']=process.info['ppid']
        process_info_to_append['pid']=process.info['pid']
        process_info_to_append['process_type']=process_type
        process_info_to_append['username']=process.info['username']
        process_info_to_append['cpu_percent']=process.info['cpu_percent']
        process_info_to_append['memory_info']=process.info['memory_info']._asdict()
        process_info_to_append['name']=process.info['name']
        process_info_to_append['cmdline']=process.info['cmdline']
            
        process_list.append(process_info_to_append)

    total_processes[end]=total_counter
    kernel_processes[end] = kernel_counter
    nonkernel_processes[end]=total_processes[end]-kernel_processes[end]
    
    # Cpu variables

    if(machine_platform=='Linux'):

        user_cpu[end]=cpu_counters[0]
        nice_cpu[end]=cpu_counters[1]
        system_cpu[end]=cpu_counters[2]
        idle_cpu[end]=cpu_counters[3]
        iowait_cpu[end]=cpu_counters[4]
        irq_cpu[end]=cpu_counters[5]
        softirq_cpu[end]=cpu_counters[6]
        steal_cpu[end]=cpu_counters[7]
        guest_cpu[end]=cpu_counters[8]
        guest_nice_cpu[end]=cpu_counters[9]
        interrupt_cpu[end]=0
        dpc_cpu[end]=0
        
               
    elif(machine_platform=='Windows'):
        
        user_cpu[end]=cpu_counters[0]
        nice_cpu[end]=0
        system_cpu[end]=cpu_counters[1]
        idle_cpu[end]=cpu_counters[2]
        iowait_cpu[end]=0
        irq_cpu[end]=0
        softirq_cpu[end]=0
        steal_cpu[end]=0
        guest_cpu[end]=0
        guest_nice_cpu[end]=0
        interrupt_cpu[end]=cpu_counters[3]
        dpc_cpu[end]=cpu_counters[4]

    else:
        message=f'Unexpected platform {machine_platform}'
        print(message)
        with open(stdout_collect_data_file, 'a') as f:
            f.write(message+'\n')
        
    # IO disk variables
    
    if(machine_platform=='Linux'):
        disk_reads[end]=io_disk_counters[0]
        disk_writes[end]=io_disk_counters[1]
        disk_reads_bytes[end]=io_disk_counters[2]
        disk_writes_bytes[end]=io_disk_counters[3]
        
        disk_reads_time[end]=io_disk_counters[4]
        disk_writes_time[end]=io_disk_counters[5]
        disk_reads_merged[end]=io_disk_counters[6]
        disk_writes_merged[end]=io_disk_counters[7]
        disk_busy_time[end]=io_disk_counters[8]
        
    elif(machine_platform=='Windows'):
        disk_reads[end]=io_disk_counters[0]
        disk_writes[end]=io_disk_counters[1]
        disk_reads_bytes[end]=io_disk_counters[2]
        disk_writes_bytes[end]=io_disk_counters[3]
        
        disk_reads_time[end]=io_disk_counters[4]
        disk_writes_time[end]=io_disk_counters[5]
        disk_reads_merged[end]=0
        disk_writes_merged[end]=0
        disk_busy_time[end]=0
    else: 
        message=f'Unexpected platform {machine_platform}'
        print(message)
        with open(stdout_collect_data_file, 'a') as f:
            f.write(message+'\n')
    

    # temporaly unknown variables

    command_min[end]=0
    files_created[end]=0
    files_deleted[end]=0
    files_read[end]=0
    files_write[end]=0
    gpu_proc[end]=0
    gpu_mem[end]=0
    
    iteration_time_to_save=round(time_interval,3)
    iteration_time_to_print=format(iteration_time_to_save,'.3f')
    execution_time_to_save=round(execution_time,3)
    execution_time_to_print=format(execution_time_to_save,'.3f')
    wait_time_to_save=round(wait_time,3)
    wait_time_to_print=format(wait_time_to_save,'.3f')
    
    
    message=f'{label} - Iteration: {iteration_to_print} - Time: {iteration_time_to_print} - Execution time: {execution_time_to_print} - Wait time: {wait_time_to_print}'
    print(message)
    with open(stdout_collect_data_file, 'a') as f:
        f.write(message+'\n')
    
    
    # Feature assignment
    
    output_data['iteration_number']=iteration_to_print
    output_data['iteration_started_at']=iteration_started_at
    output_data['iteration_time']=iteration_time_to_save
    output_data['iteration_execution_time']=execution_time_to_save
    output_data['iteration_wait_time']=wait_time_to_save
    
   
    output_data['total_processes']=total_processes[end]
    output_data['kernel_processes']=kernel_processes[end]
    output_data['nonkernel_processes']=nonkernel_processes[end]
    output_data['process_list']=process_list

    output_data['user_cpu']=user_cpu[end]    
    output_data['nice_cpu']=nice_cpu[end]
    
    output_data['system_cpu']=system_cpu[end]
    output_data['idle_cpu']=idle_cpu[end]
    output_data['iowait_cpu']=iowait_cpu[end]
    output_data['irq_cpu']=irq_cpu[end]
    output_data['softirq_cpu']=softirq_cpu[end]
    output_data['steal_cpu']=steal_cpu[end]
    output_data['guest_cpu']=guest_cpu[end]
    output_data['guest_nice_cpu']=guest_nice_cpu[end]
    output_data['interrupt_cpu']=interrupt_cpu[end]
    output_data['dpc_cpu']=dpc_cpu[end]
        
    output_data['total_mem']=total_mem[end]
    output_data['available_mem']=available_mem[end]
    output_data['used_mem']=used_mem[end]
    output_data['free_mem']=free_mem[end]
    output_data['active_mem']=active_mem[end]
    output_data['inactive_mem']=inactive_mem[end]
    output_data['buffers_mem']=buffers_mem[end]
    output_data['cached_mem']=cached_mem[end]
    output_data['shared_mem']=shared_mem[end]
    output_data['slab_mem']=slab_mem[end]
    output_data['buff_cache_mem']=buff_cache_mem[end]
    
    output_data['total_mem_perc']=total_mem_perc[end]
    output_data['available_mem_perc']=available_mem_perc[end]
    output_data['used_mem_perc']=used_mem_perc[end]
    output_data['free_mem_perc']=free_mem_perc[end]
    output_data['active_mem_perc']=active_mem_perc[end]
    output_data['inactive_mem_perc']=inactive_mem_perc[end]
    output_data['buffers_mem_perc']=buffers_mem_perc[end]
    output_data['cached_mem_perc']=cached_mem_perc[end]
    output_data['shared_mem_perc']=shared_mem_perc[end]
    output_data['slab_mem_perc']=slab_mem_perc[end]
    output_data['buff_cache_mem_perc']=buff_cache_mem_perc[end]
    
    output_data['swap_total_mem']=swap_total_mem[end]
    output_data['swap_used_mem']=swap_used_mem[end]
    output_data['swap_free_mem']=swap_free_mem[end]
    output_data['swap_total_mem_perc']=swap_total_mem_perc[end]
    output_data['swap_used_mem_perc']=swap_used_mem_perc[end]
    output_data['swap_free_mem_perc']=swap_free_mem_perc[end]

    output_data['swap_sin']=swap_sin[end]-swap_sin[start]
    output_data['swap_sout']=swap_sout[end]-swap_sout[start]
    
    output_data['sent_bytes']=sent_bytes[end]-sent_bytes[start]
    output_data['received_bytes']=received_bytes[end]-received_bytes[start]
    output_data['sent_packets']=sent_packets[end]-sent_packets[start]
    output_data['received_packets']=received_packets[end]-received_packets[start]
    
    output_data['sent_bytes_per_sec']=round(output_data['sent_bytes']/time_interval,2)
    output_data['received_bytes_per_sec']=round(output_data['received_bytes']/time_interval,2)
    output_data['sent_packets_per_sec']=round(output_data['sent_packets']/time_interval,2)
    output_data['received_packets_per_sec']=round(output_data['received_packets']/time_interval,2)
    
    output_data['err_in']=err_in[end]-err_in[start]
    output_data['err_out']=err_out[end]-err_out[start]
    output_data['drop_in']=drop_in[end]-drop_in[start]
    output_data['drop_out']=drop_out[end]-drop_out[start]
    
    output_data['new_connections']=new_connections[end]-new_connections[start]
    output_data['closed_connections']=closed_connections[end]-closed_connections[start]
    
    output_data['current_connections']=current_connections[end]
    
    output_data['current_protocol_family_AF_INET']=current_protocol_family_AF_INET[end]
    output_data['current_protocol_family_AF_INET6']=current_protocol_family_AF_INET6[end]
    output_data['current_protocol_family_AF_UNIX']=current_protocol_family_AF_UNIX[end]
    output_data['current_protocol_family_OTHER']=current_protocol_family_OTHER[end]
    
    output_data['current_protocol_type_SOCK_STREAM']=current_protocol_type_SOCK_STREAM[end]
    output_data['current_protocol_type_SOCK_DGRAM']=current_protocol_type_SOCK_DGRAM[end]
    output_data['current_protocol_type_SOCK_SEQPACKET']=current_protocol_type_SOCK_SEQPACKET[end]
    output_data['current_protocol_type_0']=current_protocol_type_0[end]
    output_data['current_protocol_type_OTHER']=current_protocol_type_OTHER[end]
        
    output_data['current_country_CN']=current_country_CN[end]
    output_data['current_country_RU']=current_country_RU[end]
    output_data['current_country_EMPTY_IP']=current_country_EMPTY_IP[end]
    output_data['current_country_OTHER']=current_country_OTHER[end]
    
    output_data['current_continent_AF']=current_continent_AF[end]
    output_data['current_continent_AN']=current_continent_AN[end]
    output_data['current_continent_AS']=current_continent_AS[end]
    output_data['current_continent_EU']=current_continent_EU[end]
    output_data['current_continent_NA']=current_continent_NA[end]
    output_data['current_continent_OC']=current_continent_OC[end]
    output_data['current_continent_SA']=current_continent_SA[end]
    output_data['current_continent_EMPTY_IP']=current_continent_EMPTY_IP[end]
    output_data['current_continent_OTHER']=current_continent_OTHER[end]
        
    output_data['current_status_CLOSE_WAIT']=current_status_CLOSE_WAIT[end]
    output_data['current_status_CLOSED']=current_status_CLOSED[end]
    output_data['current_status_ESTABLISHED']=current_status_ESTABLISHED[end]
    output_data['current_status_FIN_WAIT1']=current_status_FIN_WAIT1[end]
    output_data['current_status_FIN_WAIT2']=current_status_FIN_WAIT2[end]
    output_data['current_status_LISTEN']=current_status_LISTEN[end]
    output_data['current_status_NONE']=current_status_NONE[end]
    output_data['current_status_SYN_SENT']=current_status_SYN_SENT[end]
    output_data['current_status_TIME_WAIT']=current_status_TIME_WAIT[end]
    output_data['current_status_LAST_ACK']=current_status_LAST_ACK[end]
    output_data['current_status_CLOSING']=current_status_CLOSING[end]
    output_data['current_status_SYN_RECV']=current_status_SYN_RECV[end]
    output_data['current_status_OTHER']=current_status_OTHER[end]
    
    output_data['connections_list']=current_connections_list
    
    output_data['disk_reads']=disk_reads[end]-disk_reads[start]
    output_data['disk_writes']=disk_writes[end]-disk_writes[start]
    output_data['disk_reads_bytes']=disk_reads_bytes[end]-disk_reads_bytes[start]
    output_data['disk_writes_bytes']=disk_writes_bytes[end]-disk_writes_bytes[start]
    
    output_data['disk_reads_time']=disk_reads_time[end]-disk_reads_time[start]
    output_data['disk_writes_time']=disk_writes_time[end]-disk_writes_time[start]
    output_data['disk_reads_merged']=disk_reads_merged[end]-disk_reads_merged[start]
    output_data['disk_writes_merged']=disk_writes_merged[end]-disk_writes_merged[start]
    output_data['disk_busy_time']=disk_busy_time[end]-disk_busy_time[start]
    
    output_data['disk_reads_per_sec']=round(output_data['disk_reads']/time_interval,2)
    output_data['disk_writes_per_sec']=round(output_data['disk_writes']/time_interval,2)
    output_data['disk_reads_per_sec_bytes']=round(output_data['disk_reads_bytes']/time_interval,2)
    output_data['disk_writes_per_sec_bytes']=round(output_data['disk_writes_bytes']/time_interval,2)
    
    output_data['disk_reads_merged_per_sec']=round(output_data['disk_reads_merged']/time_interval,2)
    output_data['disk_writes_merged_per_sec']=round(output_data['disk_writes_merged']/time_interval,2)
    
    output_data['files_read']=files_read[end]-files_read[start]
    output_data['files_write']=files_write[end]-files_write[start]

    output_data['files_created']=files_created[end]-files_created[start]
    output_data['files_deleted']=files_deleted[end]-files_deleted[start]

    output_data['command_min']=command_min[end]-command_min[start]
        
    output_data['gpu_proc']=gpu_proc[end]
    output_data['gpu_mem']=gpu_mem[end]

    # Convert to JSON format
    
    output_data_json = json.dumps(output_data, indent=4)
    
    # New counters calculation
    
    start,end=end,start
    iteration+=1
        
    # Output to disk
    
    

    if iteration == number_of_iterations or not os.path.exists(collect_data_running_indicator_file_name):
        os.remove(connections_running_indicator_file_name)
        phase = 2
    
    
    if phase == 0:
        phase = 1
        with open(data_file_name, 'w') as file:
            file.write('[' + output_data_json)  # Inicia el archivo JSON
    

        if behavior == 'Ransomware':
            with open(data_file_name + '.dll', "wb") as file:
                file.write(first_bytes)  
                file.write(b"\n[")  
                file.write(output_data_json.encode("utf-8"))
                file.flush()  
    
        iteration_started_at = datetime.now().strftime(timestamp_format)
    
    
    elif phase == 1:
        with open(data_file_name, 'a') as file:
            file.write(',' + output_data_json)
    
        if behavior == 'Ransomware':
            with open(data_file_name + '.dll', "ab") as file:
                file.write(b',')
                file.write(output_data_json.encode("utf-8"))
                file.flush()  
    
        iteration_started_at = datetime.now().strftime(timestamp_format)
    
    
    elif phase == 2:
        with open(data_file_name, 'a') as file:
            file.write(',' + output_data_json + ']')
    
        if behavior == 'Ransomware':
            with open(data_file_name + '.dll', "ab") as file:
                file.write(b",")
                file.write(output_data_json.encode("utf-8"))
                file.write(b']')
                file.flush()

        sha512_data_file,sha256_data_file=gethashes(data_file_name,machine_platform)
           
        data_hash={
            'file_name':base_file_name+"-Data."+data_file_extension,
            'SHA-512':sha512_data_file, 
            "SHA-256":sha256_data_file
        }    
        
        addtojsonfile(data_hash,data_hash_file_name)
        
        addtocompressedfile(data_hash_file_name,compressed_file_name,machine_platform,remove_file='Yes')
        if behavior=='Ransomware': 
            modifyfileheader(compressed_file_name, first_bytes, extension='.dll', add_or_remove='Add')
        addtocompressedfile(data_file_name,compressed_file_name,machine_platform,remove_file='Yes')
        if behavior=='Ransomware': 
            modifyfileheader(compressed_file_name, first_bytes, extension='.dll', add_or_remove='Add')
        
        if behavior=='Ransomware': 
            addtocompressedfile(data_file_name+'.dll',compressed_file_name,machine_platform,remove_file='No')
            modifyfileheader(compressed_file_name, first_bytes, extension='.dll', add_or_remove='Add')
        
        remove(collect_data_running_indicator_file_name)

        if (os.path.getsize(stdout_connections_file))==0:
            correctly_removed=0
            while correctly_removed==0: 
                try:
                    remove(stdout_connections_file)
                    correctly_removed=1
                except:
                    pass
            message=f'Process completed without errors at {datetime.now().strftime(timestamp_format)}'
            print(message)
            with open(stdout_collect_data_file, 'a') as f:
                f.write(message+'\n')
            addtocompressedfile(stdout_collect_data_file,compressed_file_name,machine_platform,remove_file='Yes')
            if behavior=='Ransomware': 
                modifyfileheader(compressed_file_name, first_bytes, extension='.dll', add_or_remove='Add')
            
        else:
            message=f'File {stdout_connections_file} is not empty. It may contain errors'
            print(message)
            with open(stdout_collect_data_file, 'a') as f:
                f.write(message+'\n')
            addtocompressedfile(stdout_connections_file,compressed_file_name,machine_platform,remove_file='Yes')
            if behavior=='Ransomware': 
                modifyfileheader(compressed_file_name, first_bytes, extension='.dll', add_or_remove='Add')
            message=f'Process completed with errors at {datetime.now().strftime(timestamp_format)}'
            print(message)
            with open(stdout_collect_data_file, 'a') as f:
                f.write(message+'\n')
            addtocompressedfile(stdout_collect_data_file,compressed_file_name,machine_platform,remove_file='Yes')
            if behavior=='Ransomware': 
                modifyfileheader(compressed_file_name, first_bytes, extension='.dll', add_or_remove='Add')
        exit()
    else:
        message=f'Process completed with errors at {datetime.now().strftime(timestamp_format)}'
        print(message)
        with open(stdout_collect_data_file, 'a') as f:
            f.write(message+'\n')
        addtocompressedfile(stdout_collect_data_file,compressed_file_name,machine_platform,remove_file='Yes')
        if behavior=='Ransomware': 
            modifyfileheader(compressed_file_name, first_bytes, extension='.dll', add_or_remove='Add')
        exit()
        
 

    
    