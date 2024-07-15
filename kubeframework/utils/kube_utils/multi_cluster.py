""" this file should handle basic functions related to multiple clusters like initializations """
from kubeframework.utils.kube_utils.kube_cluster import KubeCluster

from kubeframework.utils.kube_utils.utils import (
    get_node_capacity,
    get_node_availability,
    construct_pod, 
    construct_service,
    generate_random_service_name,
    get_node_name
)



class MultiCluster:
    def __init__(self, contexts:list, config_path: str = None,
                 namespace: str = None):
        
        """ Initialize multiple clusters """
        self.contexts = contexts

        self.config_path = config_path
        self.node_id = 0


    def create_cluster(self, context: str, config_file_path: str = None):

        if config_file_path is None:
            self.config_file = '~/.kube/config'

        cluster = KubeCluster(context, self.config_file)

        if cluster is None:
            raise Exception('Cluster creation failed.')
        
        return cluster

    
    def create_clusters(self, context_list: list, config_file_path: str = None) -> list:
        
        if config_file_path is None:
            self.config_file = '~/.kube/config'
        
        cluster_collection = [self.create_cluster(context) for context in context_list]

        if len(cluster_collection) <= 0:
            raise Exception('No cluster created.')
        
        return cluster_collection
    
    def get_nodes_all(self, clusters_list: list) -> list:

        """

           It will create a collection of nodes in all clusters. Given that nodes in each 
           cluster are fetched using corresponding CoreV1API. Resultant is a LIST

        """

        nodes_collection = []

        for cluster in clusters_list:
            nodes = cluster.monitor.get_nodes()
            for i in range(len(nodes)):
                nodes_collection.append(nodes[i])

        return nodes_collection
    
    def get_cluster_nodes(self, cluster)-> list:
        """" This function will receive cluster object and return all the nodes in given cluster """

        nodes_in_cluster = cluster.monitor.get_nodes()
        
        nodes_list = []
        for node in nodes_in_cluster:
            node_name = get_node_name(node)
            node_info = {'node_name': node_name, 'node_id': self.node_id}
            nodes_list.append(node_info)
            self.node_id += 1

        return nodes_list

