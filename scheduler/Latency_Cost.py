from .Scheduler import *
from copy import deepcopy
from simulator.sim_environment.Datacenter import *


class Latency_Cost(Scheduler):
    def __init__(self, path, datacenter):
        super().__init__(path=path)

        self.datacenter = datacenter
        self.dataset_processed = False

    def placement(self): #containers_service: list
        """ 1. It should receive Containers_service of last decison and return the scheduling decison -> containers_services
            2. After this decsion received in main file, need to place and migrate containes in GKE to implment this decision
            3. Get the IPs of containers 
            4. generate workloadr requests
            5. Log the Evaluation metrics
        
        """
        locations = [loc[:-2] for loc in self.datacenter.locations]
        cost = 0 # we need per itertation cost. Using this we can accumulate in the end
        if not self.dataset_processed:
            p75_stats, df_p75 = self.dataset_processing()
            df_p75 = df_p75[df_p75['location'].isin(locations)] # to make sure the cluster exists
            df_p75.sort_values(by=['core', 'E2E Delay (s)'], ascending=True, inplace=True)

        prev_decision = deepcopy(self.datacenter.containers_hosts)
        prev_decision_tuple = deepcopy(self.datacenter.containers_hosts_tuple)
        containers_request_local = deepcopy(self.datacenter.containers_request)
        for id, val in enumerate(self.datacenter.containers_hosts_tuple): # index is service id, val is node id

            # extract details: can currnet location and core meet SLA? iterate over clusters to extract location where val meets

            placed = False

            for cluster in self.datacenter.clusters: 
                if val[0] == cluster['nodes'][0]['node_id']: # assuming each cluster has one node. If True, Extract the location
                    location = cluster['location']
                    break
                else:
                    location = None
            
            core = self.datacenter.containers_request[id][1]

            if (df_p75.loc[(df_p75['location'] == location[:-2]) & (df_p75['core'] == core), 'E2E Delay (s)'].values[0]) < 0.9:
                continue
            #### if current core and latency meet SLA, no change!!
            #### if not, we need to update core and/or location
            for index, row in df_p75.iterrows():
                if row['E2E Delay (s)'] < 0.9:
                    location, core = row['location'], row['core']
                    containers_request_local[id][1] = core # updating CPU for local condition check
                    for cluster in self.datacenter.clusters: # need ID of the node in shortlisted location
                        if location == cluster['location'][:-2]:
                            node_id = cluster['nodes'][0]['node_id']
                            break
                    if np.all(self.datacenter.hosts_resources_remain[node_id][1:] >= containers_request_local[id][1:]): 
                        # if true, this node can meet latency and it has capacity
                        self.datacenter.hosts_resources_remain[node_id][1:] -= containers_request_local[id][1:] # add an upper bound 30MB in here
                        self.datacenter.hosts_resources_req[node_id][1:] += containers_request_local[id][1:]
                        # now need to release old specs from the old node
                        self.datacenter.hosts_resources_req[val[0]][1:] -= self.datacenter.containers_request[id][1:]
                        self.datacenter.hosts_resources_remain[val[0]][1:] += self.datacenter.containers_request[id][1:]
                        self.datacenter.containers_request[id][1:] = containers_request_local[id][1:]
                        cost += (core/1000)
                        self.datacenter.containers_hosts_tuple[id] = (node_id, core)
                        placed = True
                        break
                    else:
                        break # it means current core and location can meet SLA but the node can not accommodate. Move to next cluster as we have only one node per cluster
                     # it means none of the core and location combinations can meet SLA. Go with current ones. we may violate SLA
            
        num_moves = sum(1 for prev, new in zip(prev_decision_tuple, self.datacenter.containers_hosts_tuple) if prev[0] != new[0])
        num_scalings = sum(1 for prev, new in zip(prev_decision_tuple, self.datacenter.containers_hosts_tuple) if prev[1] != new[1])
        
        return prev_decision_tuple, num_moves, num_scalings, cost # should we return old decision or new? new is already available
        # if we send back old and foeward it to gke_deployment, so that it should only take decisions for those where tuple mismatch
    
    
    def placement_new(self):

        locations = [loc[:-2] for loc in self.datacenter.locations]
        cost = 0 # we need per itertation cost. Using this we can accumulate in the end
        #if not self.dataset_processed:
        df_p75 = self.dataset_processing()
        df_p75 = df_p75[df_p75['location'].isin(locations)] # to make sure the cluster exists
        df_p75.sort_values(by=['core', 'E2E Delay (s)'], ascending=True, inplace=True)
        
        prev_decision_tuple = deepcopy(self.datacenter.containers_hosts_tuple)
        containers_request_local = deepcopy(self.datacenter.containers_request)

        for service_id, node_id in enumerate(self.datacenter.containers_hosts):
            location = self.datacenter.containers_hosts_obj[service_id].location
            core = self.datacenter.containers_request[service_id][1]

            if (df_p75.loc[(df_p75['location'] == location[:-2]) & (df_p75['core'] == core), 'E2E Delay (s)'].values[0]) < 0.9:
                continue
            
            for index, row in df_p75.iterrows():
                if row['E2E Delay (s)'] < 0.9:
                    location, core = row['location'], row['core']
                    containers_request_local[service_id][1] = core # updating CPU for local condition check
                    for cluster in self.datacenter.clusters: # need ID of the node in shortlisted location
                        if location == cluster['location'][:-2]:
                            new_node_id = cluster['nodes'][0]['node_id']
                            break
                    if np.all(self.datacenter.hosts_resources_remain[new_node_id][1:] >= containers_request_local[service_id][1:]): 
                        # if true, this node can meet latency and it has capacity
                        self.datacenter.hosts_resources_remain[new_node_id][1:] -= containers_request_local[service_id][1:] # add an upper bound 30MB in here
                        self.datacenter.hosts_resources_req[new_node_id][1:] += containers_request_local[service_id][1:]
                        # now need to release old specs from the old node
                        self.datacenter.hosts_resources_req[node_id][1:] -= self.datacenter.containers_request[service_id][1:]
                        self.datacenter.hosts_resources_remain[node_id][1:] += self.datacenter.containers_request[service_id][1:]
                        self.datacenter.containers_request[service_id][1:] = containers_request_local[service_id][1:]
                        #cost += (core/1000)
                        self.datacenter.containers_hosts_tuple[service_id] = (new_node_id, core)
                        self.datacenter.containers_hosts[service_id] = new_node_id
                        placed = True
                        break
                    else:
                        break # it means current core and location can meet SLA but the node can not accommodate. Move to next cluster as we have only one node per cluster
                     # it means none of the core and location combinations can meet SLA. Go with current ones. we may violate SLA

        cost += np.sum(self.datacenter.containers_request[:,1]/1000)
        num_moves = sum(1 for prev, new in zip(prev_decision_tuple, self.datacenter.containers_hosts_tuple) if prev[0] != new[0])
        num_scalings = sum(1 for prev, new in zip(prev_decision_tuple, self.datacenter.containers_hosts_tuple) if prev[1] != new[1])
        print('#################   New Decision    ############')
        print(self.datacenter.containers_hosts_tuple)
        
        #self.datacenter.containers_hosts_obj = self.datacenter.kube_nodes[self.datacenter.containers_hosts]
        return prev_decision_tuple, self.datacenter.containers_hosts_obj, num_moves, num_scalings, cost # should we return old decision or new? new is already available
        # if we send back old and foeward it to gke_deployment, so that it should only take decisions for those where tuple mismatch


    #def sla_placement(self):
        
        
        
         #  TODO: WRITE AN SLA SENSITIVE SCHEDULER HERE NOW




    def dataset_processing(self):

        df = self.dataset_reading()

        df.drop(df.iloc[:, 0:5], inplace=True, axis=1)
        df.reset_index(drop=True, inplace=True)
        df = df.reindex(columns=['location', 'core', 'E2E Delay (s)'])

        p75 = df.groupby(['location', 'core'])['E2E Delay (s)'].quantile(0.75)
        df_p75 = p75.reset_index()
        p75_np = p75.unstack()

        """"
            row -> location, column -> core
            belgium, france, germany, london -> half, one, two
            [[0.85705209 0.53654194 0.45042032]
            [0.99882495 0.60448563 0.53370607]
            [0.93496609 0.58845264 0.50755984]
            [1.01564574 0.60094613 0.48922056]]"""

        # Convert the result into a numpy array
        p75_np = p75_np.to_numpy()

        return df_p75
    
    def sla_placement(self, sla_violations):
        
        locations = [loc[:-2] for loc in self.datacenter.locations]
        cost = 0 # we need per itertation cost. Using this we can accumulate in the end
        df_p75 = self.dataset_processing()
        df_p75 = df_p75[df_p75['location'].isin(locations)] # to make sure the cluster exists
        df_p75.sort_values(by=['core', 'E2E Delay (s)'], ascending=True, inplace=True)
        
        prev_decision_tuple = deepcopy(self.datacenter.containers_hosts_tuple)
        containers_request_local = deepcopy(self.datacenter.containers_request)