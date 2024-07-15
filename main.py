import click
import os
import json
from pprint import PrettyPrinter
pp = PrettyPrinter(indent=4)
from copy import deepcopy
import pathlib
from PIL import Image
import numpy as np
import time
import csv
import pandas as pd
import warnings

# Suppress FutureWarning
warnings.simplefilter(action='ignore', category=DeprecationWarning)

NUM_STEPS = 100
sla_threshold = ()
timestamp = time.time()
fieldnames = ['Time', 'Model Name', 'File Name', 'Propagation Delay (s)', 'Processing Delay (ms)', 'E2E Delay (s)', 'location', 'core']
serverfields = ['num_moves', 'num_scalings', 'cost']
csv_path = 'imp_logs_concat/experiment_logs.csv'
client_path = f'logs/eight_containers/ClientStat_{timestamp}_GeKube.csv'
server_path = f'logs/eight_containers/ServerStat_{timestamp}_GeKube.csv'

from simulator.sim_environment.Datacenter import *
from simulator.utils.constants import (
    CONFIGS_PATH
)
from workload.Workload import *
from scheduler.Scheduler import *
from scheduler.Latency_Cost import *
from scheduler.Latency import *
from scheduler.Cost import *
from scheduler.GeKube import *

# usage = 'usage: python main.py -s <scheduler>'

# @click.command()
# #@click.option('--hosts', '-h', required=True, type=int, default=6)
# #@click.option('--containers', '-c', required=True, type=int, default=20)
# @click.option('--hosts', '-h', required=False, type=int)
# @click.option('--containers', '-c', required=False, type=int)
# @click.option("--scheduler", '-s', type=str, default="random", required=True,
#               help="Choose a scheduler to run")


def initializeEnvironment(dataset_path: str):

    config_file_path = os.path.join(CONFIGS_PATH, 'datacenter.json')
    with open(config_file_path) as cf: config = json.loads(cf.read())

    pp.pprint(config)

    generator_config = deepcopy(config)
    del generator_config['notes']
    datacenter = DatacenterGeneration(**generator_config)

    datacenter.generateCluster() ## creating a collection of multi region clusters and it will generate hosts
    #datacenter.warmup()
    containers_ips, cost = datacenter.randomDeployment() # an array where index is service id and value is node id
    
    workload = WorkloadGenerator() # noting to pass for constructor
    
    sla = np.zeros(len(containers_ips))
    client_stats = []
    for idx, ip in enumerate(containers_ips):
        response = workload.client_request(ip=ip)
        if response:
            client_stats.append(response)
            response['location'] = datacenter.containers_locations[idx][:-2]
            response['core'] = datacenter.containers_request[idx][1]
            with open(csv_path, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow(response)

            with open(client_path, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow(response)

            if response['E2E Delay (s)'] > 1:
                sla[idx] += 1
    
    #scheduler = Latency_Cost(dataset_path, datacenter) # provide parameters if there are any to initialize constructor
    #scheduler = Latency(dataset_path, datacenter) 
    #scheduler = Cost(dataset_path, datacenter) 
    scheduler = GeKube(dataset_path, datacenter) 

    server_stat = {
        'num_moves': 0,
        'num_scalings': 0,
        'cost': cost
    }
    with open(server_path, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=serverfields)
                writer.writerow(server_stat)
    # workload object for client traffic
    return datacenter, workload, scheduler, client_stats, sla, server_stat


def stepExperiment(datacenter, workload, scheduler, client_stats, sla_stats):
    
    prev_decision, containers_hosts_obj, num_moves, num_scalings, cost = scheduler.placement() # it receives old decision to be forwarded to gke_deployment
    print(f'Migrations: {num_moves}, Scalings: {num_scalings}')

    containers_ips = datacenter.migrate(prev_decision, containers_hosts_obj) # It returns IPs of all the active pods

    for idx,ip in enumerate(containers_ips):
        response = workload.client_request(ip=ip)
        if response:
            client_stats.append(response)
            response['location'] = datacenter.containers_locations[idx][:-2]
            response['core'] = datacenter.containers_request[idx][1]
            with open(csv_path, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow(response)
            
            with open(client_path, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow(response)

    server_stat = {
        'num_moves': num_moves,
        'num_scalings': num_scalings,
        'cost': cost
    }
    
    with open(server_path, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=serverfields)
                writer.writerow(server_stat)

    print(f'SLA Stats: {sla_stats}')

    return server_stat, client_stats

if __name__ == "__main__":
    path = pathlib.Path(__file__).parent.resolve()
    #csv_path, fieldnames, server_path, serverfields = generate_csv(path)

    dataset_path = path / 'imp_logs_concat'
    
    server_stats = list()
    datacenter, workload, scheduler, client_stats, sla, server_stat = initializeEnvironment(str(dataset_path))
    server_stats.append(server_stat)
    for num in range(NUM_STEPS):
        print(f'Step: {num}')
        server_stat, client_stats = stepExperiment(datacenter=datacenter, workload=workload, scheduler=scheduler, client_stats=client_stats, sla_stats=sla) # why passing and receiving stats every time
        server_stats.append(server_stat)

    df_client = pd.DataFrame(client_stats)
    df_client.to_csv('client_stats_sla_v1.csv', index=False)
    df_server = pd.DataFrame(server_stats)
    df_server.to_csv('server_stats_sla_v1.csv', index=False)
        
