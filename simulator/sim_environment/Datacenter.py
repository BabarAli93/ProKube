import itertools
import random
from copy import deepcopy
import re
from kubeframework.utils.kube_utils.multi_cluster import MultiCluster
from kubeframework.utils.kube_utils.utils import (
    get_node_capacity,
    get_node_availability,
    construct_pod, 
    construct_service,
    generate_random_service_name,
    get_node_name,
    mapper
)

from kubeframework.utils.kube_utils.descriptors import(
    KubeNode,
    KubeService
)

import numpy as np


class DatacenterGeneration:
    def __init__(self, nums: dict, metrics: dict, container_conf: dict, num_steps: int,
                 datacenter_start_time: int, datacenter_end_time: int,
                 seed: int, contexts: list, image: str, config_path: str):

        #self.containers_resources_request = None
        #self.hosts_resources_cap = None
        self.seed = seed
        np.random.seed(self.seed)
        random.seed(seed)

        self.num_clusters = nums['num_clusters']
        self.num_hosts = nums['num_hosts'] ## Number of hosts or nodes
        self.hosts_per_cluster = nums['hosts_per_cluster']  
        self.num_containers = nums['num_containers']  ## number of containers or services
        self.hosts = np.empty((self.num_hosts, 3))
        self.containers_request = np.full((self.num_containers, 3), -1)
        self.hosts_resources_cap = np.full((self.num_hosts, 3), -1)
        self.hosts_resources_alloc = np.full((self.num_hosts, 3), -1)
        self.hosts_resources_remain = np.full((self.num_hosts, 3), -1)
        self.hosts_resources_req = np.full((self.num_hosts, 3), -1)
        self.container_conf = container_conf
        self.image = image
        self.pulled = False
        self.cost = 0
        self.locations = []
        self.port = 5000
        self.containers_locations = list()

        assert len(self.container_conf) == len(metrics), \
              "Contatianer can have CPU and RAM only. Provide correct configurations"

        self.num_containers_types = nums['container_types']  ## There are One type of containers at the moment
        self.num_resources = nums['resources']  ## cpu and ram. there are 2 resources
        
        self.metrics = metrics
        assert len(self.metrics) == self.num_resources, \
            "number of metrics is not equal to the number of resources"
        
        ## Need to get Containers and Hosts capacities

        self.datacenter_start_time = datacenter_start_time
        self.datacenter_end_time = datacenter_end_time
        assert self.datacenter_end_time - self.datacenter_start_time > 0
        self.num_steps = num_steps

        self.containers_hosts = np.ones(self.num_containers, dtype=int) * (-1)  # TODO to be removed
        self.containers_hosts_tuple = [(-1, -1) for _ in range(self.num_containers)] # generating (-1, -1) tuples of num_containers
        
        self.containers_ips = []

        ## make use of contexts here
        self.contexts = contexts
        self.config_path = config_path

        np.set_printoptions(suppress=True, precision=3)

    def generateCluster(self):

        """ This function should create collection of clusters 
            
            - Call Multi cluster object here
            - then call generate hosts in here to create all the hosts arrays
            - create_clusters is resulting an object of each cluster

            """
        ## we got a LIST of clusters objects
        clusters_object = MultiCluster(self.contexts, self.config_path)
        self.cluster_collection = clusters_object.create_clusters(self.contexts) # it is a LIST

        # it will get us a LIST of V1 Nodes
        self.nodes_collection = clusters_object.get_nodes_all(self.cluster_collection)
        
        
        # we are creating hosts capacities, allocatables and remaining resources arrays
        self.hosts_resources_capacities()
        self.hosts_resources_allocatable()
        self.containers_requests() # this function is used to generate containers requests

        # it extracts locations from context string
        for context in self.contexts:
            parts = context.split('_')
            for part in parts:
                if part.startswith('europe-west') and '-' in part:
                    self.locations.append(part)
                    break

        # get nodes names in a LIST
        cluster_nodes_colletion = {}
        self.nodes_to_clusters = {}
        for cluster in self.cluster_collection:
            cluster_nodes_colletion[cluster] = clusters_object.get_cluster_nodes(cluster)

            # storing node name : cluster obj (Used it for requested resources calculations)
            for id, node in enumerate(cluster_nodes_colletion[cluster]):
                self.nodes_to_clusters[node['node_name']] = cluster
                
        self.clusters = []
        for id, cluster in enumerate(self.cluster_collection):
            list_item = {'cluster_obj': self.cluster_collection[id], 'location': self.locations[id],
                                 'context': self.contexts[id], 'nodes': cluster_nodes_colletion[cluster]}
            self.clusters.append(list_item)

        kube_nodes = list()
        for id, cluster in enumerate(self.cluster_collection):
            for node in cluster_nodes_colletion[cluster]:
                kube_node = KubeNode(id=node['node_id'], location=self.locations[id], context=
                                                     self.contexts[id], cluster_obj=self.cluster_collection[id], node=self.nodes_collection[node['node_id']])
                kube_nodes.append(kube_node)

        self.kube_nodes = np.array([node for node in kube_nodes])
        
        return self.cluster_collection, self.clusters

    def generateHosts(self, nodes_list : list):

        # we have created both array and lists of hosts and containers features
        # | Node ID | Node RAM | Node CPU |
        # (Node ID , Node RAM, Node CPU)

        # In here, we are going to call kubernetes operation to create a list of hosts, their CPU and ram
        # for that use 'get_node_capacity' from utils
        
        # Multi Cluster will give us access to KubeCluster. Nodes capacities, nodes cpu and ram usage as per request model
        
        # assert len(nodes_list) == self.num_hosts, 'Number of hosts in real testbed is not equal to ones in Datacenter'
        
        # for i, node in enumerate(nodes_list):
        #     cpu_mem_dict = get_node_capacity(node)
        #     self.hosts[i,0] = i
        #     self.hosts[i,1] = cpu_mem_dict['cpu']
        #     self.hosts[i,2] = round((int(cpu_mem_dict['memory'][:-2])) / (1000 ** 2))
        
        return None

    def warmup(self):
        # deploy each image in each core container and destroy it.

        cpu, mem = 500, 1000
        for cluster in self.clusters:
            nodes_in_cluster = len(cluster['nodes'])
            for node in range(nodes_in_cluster):
                
                node_name = cluster['nodes'][node]['node_name']
                service_name = generate_random_service_name(service_id=node, node_id=node)
                #service_body = construct_service(name=service_name, port=5000, targetPort=5000)
                pod_body = construct_pod(name=service_name, image=self.image, limit_cpu=f"{cpu}m",
                                                        limit_mem=f"{mem}Mi", node_name=node_name) 
                pods = cluster['cluster_obj'].action.create_pods([pod_body])
                cluster['cluster_obj'].action.delete_pod(name=pods[0].metadata.name, migration=False, src_client=None)

        return True
    
    def randomDeployment(self) -> bool:
        # this function deploy containers on random nodes in the inital deployment phase
        # make a copy of self.hosts_resources_alloc to be used in loop here. -> Done
        cost = 0
        hosts_requests_local = self.hosts_resources_requested()
        hosts_resources_remain_local = self.hosts_resources_remaining()
        prev_decison = deepcopy(self.containers_hosts_tuple)
        
        host_list = list(np.arange(self.num_hosts))
        try:
            for container in range(self.num_containers):
                placed = False
                random.shuffle(host_list)
                for index, val in enumerate(host_list):
                    if np.all(self.hosts_resources_remain[val][1:] >= self.containers_request[container][1:]):
                        self.hosts_resources_remain[val][1:] -= self.containers_request[container][1:] # add an upper bound 30MB in here
                        self.hosts_resources_req[val][1:] += self.containers_request[container][1:]
                        #cost += (self.containers_request[container][1]/1000)
                        self.containers_hosts[container] = val
                        self.containers_hosts_tuple[container] = (val, self.containers_request[container][1]) # it will update (node, core) tuple
                        placed = True
                        #self.hosts_resources_remain = hosts_resources_remain_local
                        break
                if not placed:
                    raise Exception(f"Container {container} and rest ones could not be placed on any host")
        except Exception as e:
            print(f"An error occurred during container placement: {e}. Either increase host's capacities or "
                  f"reduce number of containers.")
        # calculate cost here. cost += self.containers_hosts[:,1]

        print(f'############    Initial Placement    ##################')
        print(self.containers_hosts_tuple)
        cost += np.sum(self.containers_request[:,1]/1000)
        print(f'Remaining resources: {self.hosts_resources_remain}')

        # This should have been done in constructor - Containers_hosts -> index is service, value is node id
        # It is similar to containers_hosts but it has KubeNode class object for same node id stored in the respective position of containers_hosts

        self.containers_hosts_obj = np.array([
            self.kube_nodes[node_id] for node_id in self.containers_hosts
        ])

        # now Make GKE deployment 
        self.initialization_new(prev_decison)
        
        # This deployment is similar to scheduler so it should return num_moves, num_scalings and cost to main
        return self.containers_ips, cost
    
    def initialization(self, prev_decision):

        for idx, (prev, new) in enumerate(zip(prev_decision, self.containers_hosts_tuple)):
            if prev != new:
                # index is the container id now and the value is (node_id, core) values
                val_check = any(num == -1 for num in new)

                if not val_check: # to make sure there is no negative value
                        # Iterate over self.clusters to match node id and then extract relevant information for YAML body
                    for cluster in self.clusters:
                        nodes_in_cluster = len(cluster['nodes'])
                        for node in range(nodes_in_cluster):
                            if new[0] == cluster['nodes'][node]['node_id']:
                                #TODO: Need to work around here to get only the required node if there are more than one node in a cluster
                                # TODO: Make sure val[1] has same value as f"{self.containers_request[idx][1]}m. if not then there is a problem
                                node_name = cluster['nodes'][0]['node_name']
                                service_name = generate_random_service_name(service_id=idx, node_id=new[0])
                                service_body = construct_service(name=service_name, port=5000, targetPort=5000)
                                pod_body = construct_pod(name=service_name, image=self.image, limit_cpu=f"{self.containers_request[idx][1]}m",
                                                            limit_mem=f"{self.containers_request[idx][2]}Mi", node_name=node_name) # make relative subtraction here to do not request for dynamic memory
                                cluster['cluster_obj'].action.create_pods([pod_body])
                                _, service_ip = cluster['cluster_obj'].action.create_service(service=service_body)
                                self.containers_ips.append(service_ip)
        return self.containers_ips
    

    def initialization_new(self, prev_decision):
        self.services = list()
        for service_id, node in enumerate(self.containers_hosts_obj):
            if prev_decision[service_id] != self.containers_hosts_tuple[service_id]:
                    
                name = generate_random_service_name(service_id=service_id, node_id=node.id)
                        
                pod = construct_pod(name=name, image=self.image, 
                                    limit_cpu=f"{self.containers_request[service_id][1]}m",
                                    limit_mem=f"{self.containers_request[service_id][2]}Mi",
                                    node_name=node.name)
                        
                svc = construct_service(name=name, port=self.port, targetPort=self.port)

                service = KubeService(id=service_id, pod=pod, svc=svc)
                self.services.append(service)
                self.containers_hosts_obj[service_id].cluster_obj.action.create_pod(pod)
                _, service_ip = self.containers_hosts_obj[service_id].cluster_obj.action.create_service(svc)
                self.containers_ips.append(service_ip)
                self.containers_locations.append(self.containers_hosts_obj[service_id].location)

        self.services: np.array = np.array(self.services)

        return self.containers_ips

    ## write migration function
    def migrate(self, prev_decision, prev_containers_hosts_obj):
        

        self.containers_hosts_obj = self.kube_nodes[self.containers_hosts]

        # use self.containers_hosts_obj here as it has updated nodes in it

        ## TODO: FAILED HERE FOR MIGRATION. CONSIDER BOTH SCALING AND MIGRATION SAME
            # call move pod here for scheduling steps
        for idx, (service, node) in enumerate(zip(self.services, self.containers_hosts_obj)):
            if prev_decision[idx] != self.containers_hosts_tuple[idx]:
                limits = {'memory': f"{self.containers_request[idx][2]}Mi", 
                          'cpu': f"{self.containers_request[idx][1]}m"
                          }
                pod, svc, service_ip = self.containers_hosts_obj[idx].cluster_obj.action.move_pod(
                    previousPod=service.pod,
                    previousService=service.svc,
                    to_node_name=node.name,
                    to_node_id=node.id,
                    limits = limits,
                    src_context=prev_containers_hosts_obj[idx].context
                )
  
                if pod is None:
                    raise ValueError('pod should not be None')

                if svc is None:
                    raise ValueError('svc should not be None')

                # create a service for new Pod
                service = KubeService(service.id, pod, svc)

                self.services[idx] = service
                self.containers_ips[idx] = service_ip
                self.containers_locations[idx] = node.location

            # append to the list
            #services.append(service)
            #self.services = np.array(services)
        return self.containers_ips
    

    def binpackDeployment(self):
        # TODO: Deploy nodes in binpacking fashion
        # The idea is to sort the nodes in descending order of the remaining resources.
        # Pop a node from this set; Deploy Maximum containers, When filled, pop next node
        # Assumption: self.hosts_resources_available has hosts sorted in ascending order. We will use index as ID

        host_list = list(np.arange(self.num_hosts))
        popped_hosts = []
        node = host_list.pop()
        host_list.append(node)
        #try:
        for container in range(self.num_containers):
            # sort the popped list, run the lop on this and then pop a new node if the existing fails to host the container
            pass

        #except:

        # for container in range(self.num_containers):
        #     for pop_host in popped_hosts:
        #         if np.all(self.host_resources_available(pop_host) >= self.containers_resources_request[container][1:]):
        #             self.containers_hosts = pop_host
        #         else:
        #             available_resources = self.hosts_resources_available
        #             sorted_indices = np.lexsort((available_resources[:, 1], available_resources[:, 0]))
        #             sorted_hosts = available_resources[sorted_indices].tolist()
        #             host = sorted_hosts.pop()
        #             # append the popped node into the list and use this list to the maximum
        #             popped_hosts.append(host)

        return self.containers_hosts
    
    def containers_requests(self):

        # we have created both array and lists of hosts and containers features
        # | Container ID | Container CPU | Container RAM |
        # (Container ID, Container CPU, Container RAM)

        # For nodes, we have capacities and Allocatable functions

        cpu = self.container_conf['cpu']['max']
        memory = self.container_conf['memory']['max']
        for i in range(self.num_containers):
            self.containers_request[i] = [i, cpu, memory]

        return self.containers_request


    def hosts_resources_capacities(self):

        assert len(self.nodes_collection) == self.num_hosts, 'Number of hosts in real testbed is not equal to ones in Datacenter'
        
        for i, node in enumerate(self.nodes_collection):
            cpu_mem_cap_dict = get_node_capacity(node)
            self.hosts_resources_cap[i,0] = i
            self.hosts_resources_cap[i,1] = cpu_mem_cap_dict['cpu']
            self.hosts_resources_cap[i,2] = round((int(cpu_mem_cap_dict['memory'][:-2])) / (1000 ** 2))
        
        return self.hosts_resources_cap
    
    def hosts_resources_allocatable(self):
        
        for i, node in enumerate(self.nodes_collection):
            cpu_mem_alloc_dict = get_node_availability(node)
            memory = ((int(cpu_mem_alloc_dict['memory'][:-2])) / (1000 ** 2)) * 1000
            self.hosts_resources_alloc[i] = [i, int(cpu_mem_alloc_dict['cpu'][:-1]), memory]
        return self.hosts_resources_alloc
    
    def hosts_resources_remaining(self):

        """This function provides reamining resources available on each node for allocations
           - Node Capacity - Max capacity of node provisioned
           - Node Allocatables - Resources that can be allocated out of total capacity
           - Node Remaining - Subtraction of Allocatables and Total requested ones (There 
           are resources requested by Kubernetes to run management services whicha are not considered in Alloctables) 
        """
        
        # here we will make use of requsted resources and the allocatables reosurceses to calcaulate remaining ones.
        if np.any(self.hosts_resources_remain[:, 0] == -1):
            self.hosts_resources_remain[:, 0] = self.hosts_resources_alloc[:, 0]

        self.hosts_resources_remain[:, 1:] = self.hosts_resources_alloc[:, 1:] -  self.hosts_resources_req[:, 1:]
        
        return self.hosts_resources_remain
    
    def hosts_resources_requested(self):

        # this func is supposed to get all the pods in each node
        # this function should only be called once in the start. Later, there is no need to call it again, manage things locally using arrays
        if not self.pulled:
        
            for i, node in enumerate(self.nodes_collection):
                total_cpu_requests = 0
                total_memory_requests = 0
                node_name = get_node_name(node=node)
                for key, val in self.nodes_to_clusters.items():
                    if node_name == key:
                        pods = val.monitor.get_node_pods(key)

                        if pods: 
                            for pod in pods:
                                for container in pod.spec.containers:

                                    if container.resources.requests:
                                        cpu_request = container.resources.requests.get('cpu')
                                        if cpu_request:
                                            total_cpu_requests += int(cpu_request[:-1])

                                        memory_request = container.resources.requests.get('memory')
                                        if memory_request:
                                            match = re.match(r'(\d+)([mM]?[iI]?)', memory_request)
                                            if match:
                                                memory_value = float(match.group(1))
                                                total_memory_requests += memory_value
                        else:
                            print(f'No pods found in node {node_name}')
                        
                        total_memory_requests = total_memory_requests + 40 # adding a bound on safe side after examining oN GKE Dashboard (893 vs 857)
                        print(f"Requested Resources of {node_name}: CPU: {total_cpu_requests}, Memory: {total_memory_requests}")     
                self.hosts_resources_req[i] = [i, total_cpu_requests, total_memory_requests]
            self.pulled = True
            return self.hosts_resources_req
        else:
            return self.hosts_resources_req
        
        

    @property
    def host_resources_request(self):
        """return the amount of resource requested
        on each node by the containers
        """
        hosts_resources_request = []
        for host in range(self.num_hosts):
            container_in_host = np.where(self.containers_hosts == host)[0]
            host_resources_usage = sum(self.containers_resources_request[container_in_host][:, 1:])
            if type(host_resources_usage) != np.ndarray:
                host_resources_usage = np.zeros(self.num_resources)
            hosts_resources_request.append(host_resources_usage)
        return np.array(hosts_resources_request)

    @property
    def hosts_resources_available(self):  # TODO have two version for request and usage separately
        # The amount of the available. It gives resources for all the nodes
        # non-requested resources on the nodes
        return self.hosts_resources_cap[:, 1:] - self.host_resources_request

    def host_resources_available(self, host_id):  # TODO have two version for request and usage separately
        # To get the available resources For a Given Node. It is based on the requested resources Not the actual usage

        # Unfortunately, it is for simulations and it does not handle the requested resources by cluster for its management tasks

        containers_in_host = np.where(self.containers_hosts == host_id)[0]
        host_resources_usage = sum(self.containers_resources_request[containers_in_host][:, 1:])
        return self.hosts_resources_cap[host_id, 1:] - host_resources_usage

    @property
    def host_resources_available_frac(self):
        return self.hosts_resources_available / self.hosts_resources_cap[:, 1:]

    def hosted_containers(self, host_id):
        # to get the number of containers hosted in given node
        return np.count_nonzero(self.containers_hosts == host_id)

    def hosted_containers_ids(self, host_id):
        # it will return an array of containers hosted inside in a given host
        return np.where(self.containers_hosts == host_id)[0]

    def cluster_generation(self):
        return 0

    @property
    def containers_resources_usage(self):
        """return the fraction of resource usage for each node
        workload at current timestep e.g. at time step 0:
        """
        """this one is based on the usage model not the request. We will update it for dynamic use case"""
        # TODO: Update it for Dynamic use case
        self.containers_types = []
        for index, val in enumerate(self.container_types_map):
            self.containers_types.extend(itertools.repeat(index, val))
        containers_workloads = np.array(list(map(lambda container_type: self.start_workload[container_type], self.containers_types)))
        containers_resources_usage = containers_workloads * self.containers_resources_request[:, 1:]
        return containers_resources_usage
