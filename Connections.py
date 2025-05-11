import geoip2.database
import psutil
import json
import time
import os
import argparse
from datetime import datetime
from os import remove

def returncontinentandcountry(ip):
    if (ip=='EMPTY'):
        connection_continent = 'EMPTY_IP'
        connection_country = 'EMPTY_IP'
    else: 
        try:
            location=geoip_locate.country(ip)
            connection_continent = location.continent.code
            connection_country = location.country.iso_code
        except:
            connection_continent = 'OTHER'
            connection_country = 'OTHER'
    return(connection_continent,connection_country)

def returnconnectionidentifierwithstatus(connection):
    try:
        protocol_family=connection[1]
    except:
        protocol_type='EMPTY'
    try:
        protocol_type=connection[2]
    except:
        protocol_type='EMPTY'
    try:
        source_ip=connection[3][0]
    except:
        source_ip='EMPTY'
    try:
        source_port=connection[3][1]
    except:
        source_port='EMPTY'
    try:
        destination_ip=connection[4][0]
    except:
       connection_continent='EMPTY_IP'
       connection_country='EMPTY_IP'
       destination_ip='EMPTY'

    if destination_ip=='EMPTY':
        pass
    else:
        connection_continent,connection_country=returncontinentandcountry(destination_ip)
    try:
        destination_port=connection[4][1]
    except:
        destination_port='EMPTY'
    try:
         status=connection[5]
    except:
        status='EMPTY'
    if (destination_ip=='EMPTY' and destination_port!='EMPTY'):
        print(connection)
    connection_identifier_status=(protocol_family,protocol_type,source_ip,source_port,destination_ip,destination_port,connection_continent,connection_country,status)
    return (connection_identifier_status)

def addconnections (all_connections,old_current_connections):
    current_connections=set()
    current_connections_status=set()
    for connection in psutil.net_connections(kind='all'):
        temporal_coonection=returnconnectionidentifierwithstatus(connection)
        connection_to_add_status={(temporal_coonection[0],temporal_coonection[1],temporal_coonection[2],temporal_coonection[3],temporal_coonection[4],temporal_coonection[5],temporal_coonection[6],temporal_coonection[7],temporal_coonection[8]),}
        connection_to_add={(temporal_coonection[0],temporal_coonection[1],temporal_coonection[2],temporal_coonection[3],temporal_coonection[4],temporal_coonection[5],temporal_coonection[6],temporal_coonection[7],''),}
        current_connections|=connection_to_add
        current_connections_status|=connection_to_add_status
    new_connections=current_connections-old_current_connections
    removed_connections=old_current_connections-current_connections
    all_connections |= current_connections
    if(len(current_connections)!=len(current_connections_status)):
        print('Different number of connections')
    return(current_connections,current_connections_status,new_connections,removed_connections,all_connections)

def getgeographicdistribution(connections,connections_distribution):
    continent_count={'OTHER': 0}
    country_count={'OTHER': 0}
    countries=connections_distribution['countries']
    continents=connections_distribution['continents']
    for connection in connections:
        connection_continent=connection[6]
        connection_country=connection[7]
        if connection_country in countries:
            country_count[connection_country]=country_count.get(connection_country,0)+1
        else:
            country_count['OTHER']=country_count.get('OTHER',0)+1
        if connection_continent in continents:
            continent_count[connection_continent]=continent_count.get(connection_continent,0)+1
        else:
            continent_count['OTHER']=continent_count.get('OTHER',0)+1
            
    return(continent_count,country_count)

def getprotocoldistribution(connections,connections_distribution):
    protocol_family_count={'OTHER': 0}
    protocol_type_count={'OTHER': 0}
    protocol_families=connections_distribution['protocol_families']
    protocol_types=connections_distribution['protocol_types']
    for connection in connections:
        try: 
            connection_protocol_family=connection[0].name
        except:
            print(f'Protocol Family not Expected - Time: {datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}')
            print(connection)
            connection_protocol_family=connection[0]
        try:
            connection_protocol_type=connection[1].name
        except:
            print(f'Protocol Type not Expected - Time: {datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}')
            print(connection)
            connection_protocol_type=connection[1]
               
        if connection_protocol_family in protocol_families:
            protocol_family_count[connection_protocol_family]=protocol_family_count.get(connection_protocol_family,0)+1
        else:
            print(f'Protocol Family not Expected - Time: {datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}')
            print(connection_protocol_family)
            protocol_family_count['OTHER']=protocol_family_count.get('OTHER',0)+1
        if connection_protocol_type in protocol_types:
            protocol_type_count[connection_protocol_type]=protocol_type_count.get(connection_protocol_type,0)+1
        else:
            print(f'Protocol Type not Expected - Time: {datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}')
            print(connection_protocol_type)
            protocol_type_count['OTHER']=protocol_type_count.get('OTHER',0)+1
    return (protocol_family_count,protocol_type_count)

def getstatusdistribution(connections,connections_distribution):    
    status_count={'OTHER': 0} 
    statuses=connections_distribution['statuses']
    for connection in connections:
        connection_status=connection[8]
        if connection_status in statuses:
            status_count[connection_status]=status_count.get(connection_status,0)+1
        else:
            print(f'Status not Expected - Time: {datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}')
            print(connection)
            status_count['OTHER']=status_count.get('OTHER',0)+1
    return (status_count)

def addtojsonfile(data_to_convert,json_file_name):
    data_to_convert_json = json.dumps(data_to_convert, indent=4) 
    with open(json_file_name, 'w') as file:
        file.write(data_to_convert_json)    

# Input parameters definition

parser = argparse.ArgumentParser(description='Connections Data Collection Script')
parser.add_argument('--connections_running_indicator_file_name', type=str, default='CollectConnections.running',help='Running indicator file name to use')
parser.add_argument('--connections_want_to_read_indicator_file_name', type=str, default='CollectConnections.want_to_read',help='Parent wants to read indicator file name')
parser.add_argument('--connections_output_data_file_name', type=str, default='connections.json',help='File for data output')
parser.add_argument('--connections_reading_control', type=str, default='Semaphore',help='Connections file reading control type')
parser.add_argument('--geoip_database_name', type=str, default='GeoLite2-Country.mmdb',help='Geoip2 database file name')
parser.add_argument('--timestamp_format', type=str, default='%Y-%m-%dT%H:%M:%S.%fZ', help='Timestamp format to use')
parser.add_argument('--connections_cycle_time', type=str, default='250', help='Default cycle time in miliseconds')
parser.add_argument('--debug_mode', type=str, default='NoDebug', help='Default sleep duration in miliseconds')
parser.add_argument('--connections_distribution', type=str, default='', help='Default distributions')
parser.add_argument('--connection_counters', type=str, default='', help='Default counters')
args = parser.parse_args()
    
# Main Code

connections_running_indicator_file_name =args.connections_running_indicator_file_name 
connections_want_to_read_indicator_file_name=args.connections_want_to_read_indicator_file_name
connections_reading_control=args.connections_reading_control
connections_output_data_file_name=args.connections_output_data_file_name
geoip_database_name=args.geoip_database_name
timestamp_format=args.timestamp_format
connections_cycle_time=float(args.connections_cycle_time)/1000
debug_mode=args.debug_mode
if args.connections_distribution=='':
    connections_distribution={
        'countries': ('CN','RU','CU','VE','KP','IR','EMPTY_IP'),
        'continents':('AF','AN','AS','EU','NA','OC','SA','EMPTY_IP'),
        'statuses':('ESTABLISHED','NONE','SYN_SENT','TIME_WAIT','CLOSE_WAIT','FIN_WAIT1','FIN_WAIT2','CLOSED','LISTEN','LAST_ACK','CLOSING','SYN_RECV'),
        'protocol_families':('AF_INET','AF_INET6','AF_UNIX'),
        'protocol_types':('SOCK_STREAM','SOCK_DGRAM','SOCK_SEQPACKET','0')
        }
else:
    connections_distribution=json.loads(args.connections_distribution)
if args.connection_counters=='':
    connection_counters={
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
        }
else:
    connection_counters=json.loads(args.connection_counters)
    

geoip_locate=geoip2.database.Reader(geoip_database_name)
all_connections=set()
num_all_connections=0
current_connections=set()
num_closed_connections=0
iteration_number=0
with open(connections_running_indicator_file_name ,'w'):
    if(debug_mode=='Debug'):
       print(f'File {connections_running_indicator_file_name } created. If you want to stop collector process delete it')
process_started_at=datetime.now().strftime(timestamp_format)
process_chronometer_start=time.time()
while os.path.isfile(connections_running_indicator_file_name )==True:
    iteration_started_at=datetime.now().strftime(timestamp_format)
    iteration_chronometer_start=time.time()
    

    iteration_number+=1
    (current_connections,current_connections_status,new_connections,removed_connections,all_connections)=addconnections(all_connections,current_connections)
    current_connections_continents,current_connections_countries=getgeographicdistribution(current_connections,connections_distribution)
    new_connections_continents,new_connections_countries=getgeographicdistribution(new_connections,connections_distribution)

    removed_connections_continents,removed_connections_countries=getgeographicdistribution(removed_connections,connections_distribution)
    
    current_connections_protocol_families,current_connections_protocol_types=getprotocoldistribution(current_connections,connections_distribution)
    
    new_connections_protocol_families,new_connections_protocol_types=getprotocoldistribution(new_connections,connections_distribution)
    
    removed_connections_protocol_families,removed_connections_protocol_types=getprotocoldistribution(removed_connections,connections_distribution)
    
    current_connections_statuses=getstatusdistribution(current_connections_status,connections_distribution)
    
    connection_counters['current']['num_current_connections']=len(current_connections)
    
    connection_counters['all']['num_all_connections']+=+len(new_connections)
    
    connection_counters['closed']['num_closed_connections']+=len(removed_connections)
    
 
    for continent in connections_distribution['continents']+['OTHER']:
        
        try: 
            connection_counters['current']['continents'][continent]=current_connections_continents[continent]
        except: 
            connection_counters['current']['continents'][continent]=0
        try: 
            connection_counters['all']['continents'][continent]+=new_connections_continents[continent]
        except: 
            pass
        try: 
            connection_counters['closed']['continents'][continent]+=removed_connections_continents[continent]
        except: 
            pass
        
    for country in connections_distribution['countries']+['OTHER']:
        
        try: 
            connection_counters['current']['countries'][country]=current_connections_countries[country]
        except: 
            connection_counters['current']['countries'][country]=0
        try: 
            connection_counters['all']['countries'][country]+=new_connections_countries[country]
        except: 
            pass
        try: 
            connection_counters['closed']['countries'][country]+=removed_connections_countries[country]
        except: 
            pass
    connection_counters['closed']['statuses']['CLOSED']=connection_counters['closed']['num_closed_connections']
    
    
    for status in connections_distribution['statuses']+['OTHER']:
        try: 
            connection_counters['current']['statuses'][status]=current_connections_statuses[status]
        except: 
            connection_counters['current']['statuses'][status]=0
        try: 
            if (status!='CLOSED'):
                connection_counters['all']['statuses'][status]=connection_counters['current']['statuses'][status]
            else:
                connection_counters['all']['statuses'][status]=connection_counters['closed']['statuses'][status]
        except: 
            pass

    for protocol_family in connections_distribution['protocol_families']+['OTHER']:
        try: 
            connection_counters['current']['protocol_families'][protocol_family]=current_connections_protocol_families[protocol_family]
        except: 
            connection_counters['current']['protocol_families'][protocol_family]=0
        try: 
            connection_counters['all']['protocol_families'][protocol_family]+=new_connections_protocol_families[protocol_family]
        except: 
            pass
        try: 
            connection_counters['closed']['protocol_families'][protocol_family]+=removed_connections_protocol_families[protocol_family]
        except: 
            pass
    
    for protocol_type in connections_distribution['protocol_types']+['OTHER']:
        try: 
            connection_counters['current']['protocol_types'][protocol_type]=current_connections_protocol_types[protocol_type]
        except: 
            connection_counters['current']['protocol_types'][protocol_type]=0
        try: 
            connection_counters['all']['protocol_types'][protocol_type]+=new_connections_protocol_types[protocol_type]
        except: 
            pass
        try: 
            connection_counters['closed']['protocol_types'][protocol_type]+=removed_connections_protocol_types[protocol_type]
        except: 
            pass
    
    connections_list=list()
    n=0
    for connection in current_connections_status:
        n+=1
        
        connection_to_add={}
        connection_to_add['connection_count']=n
        try:
            connection_to_add['protocol_family']=connection[0].name
        except: 
            connection_to_add['protocol_family']='OTHER'
        try:
            connection_to_add['protocol_type']=connection[1].name
        except:
            connection_to_add['protocol_type']='OTHER'
        connection_to_add['source_ip']=connection[2]
        connection_to_add['source_port']=connection[3]
        connection_to_add['destination_ip']=connection[4]
        connection_to_add['destination_port']=connection[5]
        connection_to_add['continent']=connection[6]
        connection_to_add['country']=connection[7]
        connection_to_add['status']=connection[8]
        connections_list.append(connection_to_add)
    
    
    connection_counters['connections_list']=connections_list
    
    adjustment=(time.time()-iteration_chronometer_start)
    
    if((connections_cycle_time-adjustment)>0):
        time_to_sleep=connections_cycle_time-adjustment
    else:
        time_to_sleep=0
    
    time.sleep(time_to_sleep)
    iteration_ended_at=datetime.now().strftime(timestamp_format)
    iteration_chronometer_stop=time.time()
    iteration_duration=iteration_chronometer_stop-iteration_chronometer_start
    connection_counters['general']['process_started_at']=process_started_at
    connection_counters['general']['iteration_started_at']=iteration_started_at
    connection_counters['general']['iteration_ended_at']=iteration_ended_at
    connection_counters['general']['iteration_duration']=iteration_duration
    connection_counters['general']['iteration_number']=iteration_number
 
    
    if connections_reading_control=='Try':
        addtojsonfile(connection_counters,connections_output_data_file_name)
    elif connections_reading_control=='Semaphore':
        if os.path.exists(connections_want_to_read_indicator_file_name) and os.path.exists(connections_output_data_file_name):
            pass
        elif os.path.exists(connections_want_to_read_indicator_file_name) and not os.path.exists(connections_output_data_file_name):
            addtojsonfile(connection_counters,connections_output_data_file_name)
        elif not os.path.exists(connections_want_to_read_indicator_file_name) and os.path.exists(connections_output_data_file_name):
            remove(connections_output_data_file_name)
        else: 
            pass
    
    if(debug_mode=='Debug'):
        print(f'---------------------------------{iteration_number}-{iteration_duration} ---------------------------------')
        print(f'NST - NewConnections: {len(new_connections)} - ClosedConnections:{len(removed_connections)} - CurrentCconnections:{len(current_connections)} - AllConnections:{len(all_connections)}')
        print('---------------------------------------------------------------------')
        
        current_connections_count=connection_counters['current']['num_current_connections']
        difference_counter=0
        for value in connections_distribution['statuses']:
            difference_counter+=connection_counters['current']['statuses'][value]
        if difference_counter!=current_connections_count:
            print(f'Current connections :{current_connections_count}')
            print(f'Sum of stattuses{difference_counter}')
            print('--------------------------------------------------------')
            print(current_connections)
            print('--------------------------------------------------------')
            print(current_connections_status)
            print('--------------------------------------------------------')
            print(current_connections_statuses)
            
        difference_counter=0
        for value in connections_distribution['protocol_families']:
            difference_counter+=connection_counters['current']['protocol_families'][value]
        if difference_counter!=current_connections_count:
            print(f'Current connections :{current_connections_count}')
            print(f'Sum of protocol families{difference_counter}')
            print('--------------------------------------------------------')
            print(current_connections)
            print('--------------------------------------------------------')
            print(current_connections_status)
            print('--------------------------------------------------------')
            print(current_connections_protocol_families)
            
        difference_counter=0
        for value in connections_distribution['protocol_types']:
            difference_counter+=connection_counters['current']['protocol_types'][value]
        if difference_counter!=current_connections_count:
            print(f'Current connections :{current_connections_count}')
            print(f'Sum of protocol types{difference_counter}')
            print('--------------------------------------------------------')
            print(current_connections)
            print('--------------------------------------------------------')
            print(current_connections_status)
            print('--------------------------------------------------------')
            print(current_connections_protocol_types)
            
        
        difference_counter=0
        for value in connections_distribution['continents']:
            difference_counter+=connection_counters['current']['continents'][value]
        if difference_counter!=current_connections_count:
            print(f'Current connections :{current_connections_count}')
            print(f'Sum of continents{difference_counter}')
            print('--------------------------------------------------------')
            print(current_connections)
            print('--------------------------------------------------------')
            print(current_connections_status)
            print('--------------------------------------------------------')
            print(current_connections_continents)
            
        
        difference_counter=0
        for value in connections_distribution['countries']:
            difference_counter+=connection_counters['current']['countries'][value]
        if difference_counter!=current_connections_count:
            print(f'Current connections :{current_connections_count}')
            print(f'Sum of countries{difference_counter}')
            print('--------------------------------------------------------')
            print(current_connections)
            print('--------------------------------------------------------')
            print(current_connections_status)
            print('--------------------------------------------------------')
            print(current_connections_countries)
        
process_ended_at=datetime.now().strftime(timestamp_format)
process_chronometer_stop=time.time()
process_duration=process_chronometer_stop-process_chronometer_start
if(debug_mode=='Debug'):
    print(f'Process has finished after {iteration_number} iterations and {round(process_duration,2)} seconds')

try:
    remove(connections_running_indicator_file_name )
except:
    pass

try:
    remove(connections_output_data_file_name)
except:
    pass