from .Scheduler import *
from copy import deepcopy
from simulator.sim_environment.Datacenter import *


class GeKube(Scheduler):

    def __init__(self, path, datacenter):
        super().__init__(path=path)

        self.datacenter = datacenter
        self.dataset_processed = False

    def placement(self):

        #locations = [loc[:-2] for loc in self.datacenter.locations]
        cost = 0 # we need per itertation cost. Using this we can accumulate in the end
        cores, locs = self.dataset_processing()
        prev_decision_tuple = deepcopy(self.datacenter.containers_hosts_tuple)
        containers_request_local = deepcopy(self.datacenter.containers_request)

        for service_id, node_id in enumerate(self.datacenter.containers_hosts):
            curr_location = self.datacenter.containers_hosts_obj[service_id].location
            curr_core = self.datacenter.containers_request[service_id][1]
            placed=False

        #for c in cores.index:
            c = cores.index[0]
            for l in locs.index:
                if ((c == curr_core) & (l == curr_location[:-2])):
                    break
                proc= cores[c]/1000
                prop = locs[l]
                #if proc + prop < 0.9:
                containers_request_local[service_id][1] = c

                for cluster in self.datacenter.clusters: # need ID of the node in shortlisted location
                    if l == cluster['location'][:-2]:
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
                    self.datacenter.containers_hosts_tuple[service_id] = (new_node_id, c)
                    self.datacenter.containers_hosts[service_id] = new_node_id
                    placed = True
                    break
                else:
                    break 
        #    if placed:
        #        break

        cost += np.sum(self.datacenter.containers_request[:,1]/1000)
        num_moves = sum(1 for prev, new in zip(prev_decision_tuple, self.datacenter.containers_hosts_tuple) if prev[0] != new[0])
        num_scalings = sum(1 for prev, new in zip(prev_decision_tuple, self.datacenter.containers_hosts_tuple) if prev[1] != new[1])
        print('#################   New Decision    ############')
        print(self.datacenter.containers_hosts_tuple)
        
        #self.datacenter.containers_hosts_obj = self.datacenter.kube_nodes[self.datacenter.containers_hosts]
        return prev_decision_tuple, self.datacenter.containers_hosts_obj, num_moves, num_scalings, cost # should we return old decision or new? new is already available

    def dataset_processing(self):

        df = self.dataset_reading()
        locations = [loc[:-2] for loc in self.datacenter.locations]
        df = df[['location', 'core', 'Propagation Delay (s)', 'Processing Delay (ms)']]
        df = df[df['location'].isin(locations)] 
        df.reset_index(drop=True, inplace=True)

        core = df.groupby(['core'])['Processing Delay (ms)'].quantile(0.99).sort_index(ascending=True)
        loc = df.groupby(['location'])['Propagation Delay (s)'].quantile(0.99).sort_values(ascending=True)

        return core, loc