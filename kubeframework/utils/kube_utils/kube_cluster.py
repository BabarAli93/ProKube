from kubernetes.client.rest import ApiException
from kubernetes.client import (
    V1ResourceRequirements,
    CustomObjectsApi,
    AppsV1Api,
    V1ObjectMeta,
    V1Container,
    V1Namespace,
    V1PodSpec,
    CoreV1Api,
    V1Pod,
    V1Node,
    V1Deployment,
    V1Service,
    V1ServiceSpec,
    V1ServicePort
)
import json
from kubernetes import config, stream

from simulator.utils import logger
from kubeframework.utils.kube_utils.utils import (
    get_pod_name,
    get_node_name,
    get_service_name,
    generate_random_service_name
)
from typing import List, Any, Tuple
import requests
import time
import os


class BaseFunctionalities:
    """Base Class of Funtionalities

    Mainly to create the namespace as it is shared in the cluster"""

    NAMESPACE_ACTIVE = 'Active'
    POD_RUNNING = 'Running'
    POD_FAILED = 'Failed'

    def __init__(self,
                 API: CoreV1Api,
                 ObjectAPI: CustomObjectsApi,
                 AppsAPI: AppsV1Api,
                 namespace: str
                 ):
        """BaseFunctionalities Constructor

                :param API: CoreV1Api
                    get API of kubernetes for accessing by general api

                :param ObjectAPI: CustomObjectsApi
                    get API of kubernetes for accessing by object

                :param namespace: str
                    name of used namespace"""

        # CoreV1Api interface: Core API for general functions
        self.core_api: CoreV1Api = API

        # CustomObjectApi interface for Resource Monitoring purpose
        self.obj_api: CustomObjectsApi = ObjectAPI
        
        # Apps API for "Deployment"
        self.apps_api: AppsV1Api =  AppsAPI

        # Define using namespace
        self.namespace: str = namespace

    def check_namespace(self, namespace: str):
        """Check namespace

                if namespace does not exist, it will be created.

                :param namespace: str
                    name of namespace
                """

        try:
            ns = self.core_api.read_namespace(namespace)
            return ns
        except ApiException as e:
            logger.warn("Namespace {} does not exist. It will be created".format(namespace))

        logger.info("Creating namespace {}".format(namespace))

        try:
            self.core_api.create_namespace(V1Namespace(metadata=V1ObjectMeta(name=namespace)))
        except ApiException as e:
            logger.error(e)

        logger.info('Waiting for namespace "{} creation"'.format(
            namespace))

        while True:
            time.sleep(1)
            try:
                ns = self.core_api.read_namespace(namespace)
                if ns.status.phase == self.NAMESPACE_ACTIVE:
                    logger.info("Namespace {} is successfully created".format(namespace))
                    return ns
            except ApiException as e:
                logger.error(e)
                logger.info("Failed to create namespace {}".format(namespace))


class Monitor(BaseFunctionalities):
    """Monitoring Functionalities

            Monitoring a kubernetes cluster
            Monitor the resource usage!
        """

    def __init__(self,
                 API: CoreV1Api,
                 ObjectAPI: CustomObjectsApi,
                 AppsAPI: AppsV1Api,
                 namespace: str
                 ):
        """Monitor Constructor

        :param API: CoreV1Api
            get API of kubernetes for accessing by general api

        :param ObjectAPI: CustomObjectsApi
            get API of kubernetes for accessing by object

        :param namespace: str
            name of used namespace
        """
        super().__init__(API, ObjectAPI, AppsAPI, namespace)

    def get_nodes(self) -> Tuple[List[V1Node], V1Node]:
        """To get the list of all the computational nodes/ Worker Nodes
        we are not considering the master node in here becsue GKE handles the master node itself
        """

        nodes = [node for node in sorted(self.core_api.list_node().items, key=lambda node: node.metadata.name)
                 if 'master' not in node.metadata.name]

        try:
            return nodes
        except ApiException as e:
            logger.error(e)

        return None
    
    def get_node_pods(self, node_name: str, namespace: str = None):

        if namespace is None:
            namespace = self.namespace

        try:
            pods = self.core_api.list_pod_for_all_namespaces(field_selector=f'spec.nodeName={node_name}').items
            return pods
        except ApiException as e:
            logger.error(e)

        return None

    def get_pod(self, name: str, namespace: str = None) -> V1Pod:
        """Get specific Pod

                :param name: str
                    name of pod

                :param namespace: str
                    namespace of pod
                """
        if namespace is None:
            namespace = self.namespace

        try:
            return self.core_api.read_namespaced_pod(name, namespace)
        except ApiException as e:
            logger.error(e)

        return None

    def get_pods(self, namespace: str = None):
        """Get all the Pods in given namespace

                :param name: str
                    name of pod

                :param namespace: str
                    namespace of pod
                """
        if namespace is None:
            namespace = self.namespace

        try:
            return self.core_api.list_namespaced_pod(namespace).items
        except ApiException as e:
            logger.error(e)
        return None

    def get_services(self, namespace: str = None):
        if namespace is None:
            namespace = self.namespace

        try:
            return self.core_api.list_namespaced_service(namespace).items
        except ApiException as e:
            logger.error(e)

        return None

    def get_pod_service(self, pod_name: str, namespace: str = None) -> List[V1Service]:

        if namespace is None:
            namespace = self.namespace

        try:
            pod = self.core_api.read_namespaced_pod(pod_name, namespace)
            pod_labels = pod.metadata.labels

            svc_selector = ','.join([f'{key}={value}' for key, value in pod_labels.items()])

            services = self.core_api.list_namespaced_service(namespace=namespace, label_selector=svc_selector)
            if services.items:
                return services.items[0]
            else:
                logger.info(f"No services is associated with pod {pod.metadata.name}")
        except ApiException as e:
            logger.error(e)

        return None

    def get_pods_metrics(self, namespace: str = None):

        """Get Pod Metrics

                :param namespace: str
                    get pod metrics of a specific namespace
                """

        def containers(metrics):
            # this function will accumulatively get the metrics of all the containers inside a pod

            items = metrics.get('items')
            conts = {
                item.get('metadata').get('name'):
                    item.get('containers')[0].get('usage')
                # we can extend container['usage']['cpu'] to get specific matric
                for item in items if len(item.get('containers')) > 0
            }
            return conts

        if namespace is None:
            namespace = self.namespace

        try:
            while True:
                metrics = self.obj_api.list_namespaced_custom_object(
                    group='metrics.k8s.io',
                    version='v1beta1',
                    namespace=namespace,
                    plural='pods'
                )

                if len(metrics.get('items')) > 0:
                    return containers(metrics)

                time.sleep(1)

        except ApiException as e:
            logger.error(e)

        return None

    def get_pod_metrics(self, pod_name: str, namespace: str = None):
        """Get Pod Metrics

                :param pod_name: str
                    name of pod

                :param namespace: str
                    get pod metrics of a specific namespace

                we need pod name as input to get matrics of a specific pod using this function
                """
        if namespace is None:
            namespace = self.namespace

        try:
            return self.obj_api.get_namespaced_custom_object('metrics.k8s.io', 'v1beta1', namespace, 'pods',
                                                             pod_name) \
                .get('containers') \
                .pop() \
                .get('usage')
        except ApiException as e:
            logger.error(e)
        return None

    def get_pod_metrics_top(self, pod_name: str, namespace: str = None):

        """This function is an extension of get_pod_metrics()
        it does processing on extracted results to present like 'kubectl top' """
        if namespace is None:
            namespace = self.namespace

        try:
            container_usage = self.obj_api.get_namespaced_custom_object('metrics.k8s.io', 'v1beta1', namespace, 'pods',
                                                             pod_name).get('containers').pop().get('usage')
            cpu_usage_nano = int(container_usage['cpu'][:-1])  # Remove the 'n' suffix
            memory_usage_kib = int(container_usage['memory'][:-2])  # Remove the 'Ki' suffix

            cpu_usage_mili = str(int(round(cpu_usage_nano / 1000000))) + 'm'

            memory_usage_mib = str(int(round(memory_usage_kib / 1024,0))) + 'Mi'

            return {'cpu': cpu_usage_mili, 'memory': memory_usage_mib}

        except ApiException as e:
            logger.error(e)
        return None

    def get_nodes_metrics(self):

        def node_metrics(nodes):
            return {
                node.get('metadata').get('name'): node.get('usage') for node in nodes
            }

        try:
            return node_metrics(self.obj_api.list_cluster_custom_object(
                group='metrics.k8s.io', version='v1beta1', plural='nodes').get('items'))
        except ApiException as e:
            logger.error(e)

        return None

    def get_node_metrics(self, node_name: str):

        """Get Node Metrics

                :param node_name: str
                    name of node
                """

        try:
            return self.obj_api.get_cluster_custom_object(
                group='metrics.k8s.io', version='v1beta1', plural='nodes',
                name=node_name).get('usage')
        except ApiException as e:
            logger.error(e)

        return None

    def get_node_metrics_top(self, node_name: str):

        """Get Node Metrics

                :param node_name: str
                    name of node
                """
        try:

            node_usage = self.obj_api.get_cluster_custom_object(
                group='metrics.k8s.io', version='v1beta1', plural='nodes',
                name=node_name).get('usage')

            node_cpu_nano = int(node_usage['cpu'][:-1])  # Remove the 'n' suffix
            node_mem_kib = int(node_usage['memory'][:-2])  # Remove the 'Ki' suffix

            node_cpu_mili = str(int(round(node_cpu_nano / 1000000))) + 'm'
            node_mem_mib = str(int(round(node_mem_kib / 1024, 0))) + 'Mi'

            return {'cpu': node_cpu_mili, 'memory': node_mem_mib}
        except ApiException as e:
            logger.error(e)

        return None

    def get_vpa_recommendation(self, namespace: str = None):

        if namespace is None:
            namespace = self.namespace

        try:
            # Fetch VPAs
            vpas = self.obj_api.list_namespaced_custom_object(
                "autoscaling.k8s.io", "v1", namespace, "verticalpodautoscalers"
            )
            # Method 1:
            #return vpas['items'][0]['status']['recommendation']['containerRecommendations']

            # Method 2:
            recommendations = {}
            for vpa in vpas.get('items', []):
                vpa_name = vpa.get('metadata', {}).get('name', '')
                if vpa_name:
                    # Fetch recommendations
                    container_recommendations = vpa.get('status', {}).get('recommendation', {}).get(
                        'containerRecommendations', [])
                    if container_recommendations:
                        # Assuming there's only one container recommendation per VPA
                        recommendations[vpa_name] = container_recommendations[0]

            return recommendations

        except ApiException as e:
            logger.error('Error retrieving VPA recommendations {}'.format(e))

        return None


class Action(BaseFunctionalities):
    """Kubernetes Cluster - Actions

    Add all the required functionalities in here.
    Adding few, If needed more, append in here
    """

    UTILIZATION_NODE_PORT = 30000

    DATA_DESTINATION = "/"

    def __init__(self,
                 API: CoreV1Api,
                 ObjectAPI: CustomObjectsApi,
                 AppsAPI: AppsV1Api,
                 namespace: str,
                 utilization_server_image: str,
                 node: V1Node,
                 config_file, 
                 cleaning_after_exiting: bool = False,
                 ):
        # TODO: I had remove stuff in comparison to SmartKube, Add when needed.
        # TODO: Consider file 'smart-kube/smart_scheduler/src/smart_scheduler/util/kubernetes_utils/cluster.py'

        """Actions Constructor

                :param API: CoreV1Api
                    get API of kubernetes for accessing by general api

                :param ObjectAPI: CustomObjectsApi
                    get API of kubernetes for accessing by object for Metrics Monitoring

                :param namespace: str
                    name of used namespace: default is 'prokube'

                :param workloads_path: str
                    path of workloads

                :param cluster_path: str
                    path of cluster

                :param utilization_server_image: str
                    using this image for utilization server

                :param cleaning_after_exiting: bool (default: False)
                    clean the cluster after exiting
                """

        super().__init__(API, ObjectAPI, AppsAPI, namespace)

        
        # TODO: How to provide different images?
        # this variable will be used to get external IP for connecting to the sdghafouri/utilization-server-smart-scheduler
        self.node: V1Node = node

        # It may be used to delete the cluster at the end for Termination signal
        self.cleaning_after_exiting: bool = cleaning_after_exiting

        # We do not have utilization image.
        # TODO: get a list of images
        self.utilization_server_image: str = utilization_server_image

        # self.workloads_path: str = workloads_path

        # self.cluster_path: str = cluster_path

        if config_file is None:
            # set default value for config_file
            self.config_file = '~/.kube/config'

        if self.cleaning_after_exiting:
            self.setup_signal()

        # Not implementing the Utilization server at the moment
        # setup utilization server
        # self._setup_utilization_server(image=self.utilization_server_image)

    def existing_pods(self):
        """ This function will provide the list of names of existing pods.
            It is needed to make sure the pods have unique names
            """

        pods_names = list(map(lambda pod: pod.metadata.name,
                              self.core_api.list_namespaced_pod(namespace=self.namespace).items))
        return pods_names
    
    def existing_deployments(self):

        deployments_names = list(map(lambda deployment: deployment.metadata.name,
                                     self.apps_api.list_namespaced_deployment(namespace=self.namespace).items))

        return deployments_names
    

    def setup_signal(self):
        """Setting Up signals
        It is defined to handle the termination based on signals like CTRL+C
        """
        logger.info('setting up signal handlers...')
        import signal
        signal.signal(signal.SIGTERM, self.exiting)
        signal.signal(signal.SIGINT, self.exiting)

    def exiting(self, signum, frame):
        """Exiting function
            It will be triggered after catch the kill or terminate signals
            , and clean the cluster.

            This function implements the functionality to clean the stuff after the termination signals
        """

        logger.info('clean the cluster in exiting...')
        self.clean()
        exit(0)


    def initialize_client(self, context: str):
        """ It will return CoreAPIObject - Primarily for context based 
        cross cluster migrations

        :param context: str
            GKE cluster context 
        """
        config.load_kube_config(context=context)
        return CoreV1Api()


    def create_deployment(self, deployment: V1Deployment, namespace: str = None):

        """ This function implements Deployment and create the asked Pods

        :param deployment: V1Deployment
            Deployment YAML/ Body

        :param namespace: str
            namespace of Pods
        """

        if namespace is None:
            # set default value for namespace
            namespace = self.namespace

        self.check_namespace(namespace)
        
        try: 

            new_dep_name = deployment.metadata.name
            logger.info(f"Creating new deployment {new_dep_name}")

            if new_dep_name in self.existing_deployments():
                raise Exception(
                    f"A deployment with name <{new_dep_name}>"
                    f" is already on the namespace <{namespace}>"
                    f", existing deployments in the namespace: {self.existing_deployments()}"
                    " try a name not already in the existing deployments!"
                )
            
            self.apps_api.create_namespaced_deployment(namespace=namespace, body=deployment)

            logger.info("Waiting for new deployment {} to run...".format(
                new_dep_name
            ))

            while True:
                
                time.sleep(1)
                
                deployment_status = self.apps_api.read_namespaced_deployment_status(
                    namespace=namespace, name=new_dep_name
                )
                
                if deployment_status.status.ready_replicas == deployment_status.status.replicas:
                    logger.info("Deployment {} is successfully running".format(new_dep_name))
                    return deployment
                # elif deployment_status.status.replicas != deployment_status.status.available_replicas:
                #     logger.info(
                #         "Deployment {} is not yet fully available. "
                #         "Ready replicas: {}, Desired replicas: {}".format(
                #             new_dep_name,
                #      POD_RUNNING   )
                #     )
                elif deployment_status.status.replicas == 0:
                    raise Exception("Deployment {} has no replicas running".format(new_dep_name))
                
                # elif deployment_status.status.unavailable_replicas:
                #     raise Exception("Deployment {} has pods failing to create or start".format(new_dep_name))
            
        except ApiException as e:
            logger.error("Deployment creation failed with error {}".format(e))

        return None

    def create_pods(self, pods: List[V1Pod], namespace: str = None):
        """Create multiple Pods

        :param pods: List[V1Pod]
            a list of pods for creation

        :param namespace: str
            namespace of Pods
        """

        if namespace is None:
            # set default value for namespace
            namespace = self.namespace

        # create all pods
        pods_list = [self.create_pod(pod=pod, namespace=namespace) for pod in pods]

        # check all pods created or not
        if None in pods_list:
            raise Exception('pods did not create.')

        return pods_list

    def create_pod(self, pod: V1Pod, namespace: str = None):
        """Create a Pod

        :param pod: V1Pod
            pod object

        :param namespace: str
            namespace of Pod
        """
        # Step 1 is to create namespace if not existed

        if namespace is None:
            namespace = self.namespace

        self.check_namespace(namespace)

        try:
            # Step 2: get pod name from the provided YAML/ Body and check the Uniqueness
            new_pod_name = pod.metadata.name
            logger.info(f'Creating pod {new_pod_name}')

            # Make sure the name is Unique and no two pods has same name
            if new_pod_name in self.existing_pods():
                raise Exception(
                    f"A pod with name <{new_pod_name}>"
                    f" is already on the namespace <{namespace}>"
                    f", existing pods in the namespace: {self.existing_pods}"
                    " try something name not already in the existing pods")
            
            #if migration is True and dest_client is not None:
             #   dest_client.create_namespaced_pod(namespace, pod)
            #else:
            self.core_api.create_namespaced_pod(namespace, pod)

            logger.info("Waiting for new pod {} to run...".format(
                new_pod_name
            ))

            while True:  # TODO points of change for parallel creation
                # step 3: Make sure the newly created pod is running
                time.sleep(3)
                #if migration:
                 #   pod = dest_client.read_namespaced_pod(namespace=namespace, name=pod.metadata.name)
                #else: 
                pod = self.core_api.read_namespaced_pod(namespace=namespace, name=pod.metadata.name)
                if pod.status.phase == self.POD_RUNNING:
                    logger.info("Pod {} is running".format(pod.metadata.name))
                    return pod
                elif pod.status.phase == self.POD_FAILED:
                    raise Exception("{} , try another initial placement".format(
                        pod.status.message
                    ))
                elif pod.status.conditions and pod.status.conditions[0].reason:
                    if pod.status.conditions[0].reason == 'Unschedulable':
                        raise Exception("pod is unschdulable {}".format(
                            pod.status.conditions[0].message
                        ))

        except ApiException as e:
            logger.error("Pod creation failed with error {}".format(e))

        return None

    def create_service(self, service: V1Service, namespace: str = None):

        """Create a Service

            :param service: V1Service
                pod object

            :param namespace: str
                namespace of service
                """
        if namespace is None:
            # set default value for namespace
            namespace = self.namespace

        # check namespace
        self.check_namespace(namespace)

        try:
            logger.info("Waiting for service {} to run...".format(get_service_name(service)))

            #if migration is True and dest_client is not None:
             #   service = dest_client.create_namespaced_service(namespace, service)
            #else:
            service =  self.core_api.create_namespaced_service(namespace, service)

            while True:
                service_name = service.metadata.name
                service_details = self.core_api.read_namespaced_service(namespace=namespace, name=service_name)
                if service_details.status.load_balancer.ingress:
                    external_ip = service_details.status.load_balancer.ingress[0].ip
                    if external_ip:
                        return service, external_ip, 
                time.sleep(2)

        except ApiException as e:
            logger.error(e)

        return None

    def create_services(self, services: List[V1Service], namespace: str = None):
        """Create multiple Services

            :param services: List[V1Services]
                a list of services for creation

            :param namespace: str
                namespace of services
        """
        if namespace is None:
            namespace = self.namespace

        _services = [self.create_service(service, namespace) for service in services]

        # check all pods created or not
        if None in _services:
            raise Exception('services did not create.')

        return _services

    def delete_pod(self, name: str,  migration: bool = False,
                    src_client: CoreV1Api = None, namespace: str = None) -> bool:

        if namespace is None:
            namespace = self.namespace

        try:
            if migration is True and src_client is not None:
                logger.info("Trying to delete pod {}".format(name))
                src_client.delete_namespaced_pod(name, namespace)
            else:
                self.core_api.delete_namespaced_pod(name, namespace)
        except ApiException as e:
            logger.error(f"Exception when deleting pod: {e}")

        try:
            while True:
                if migration is True and src_client is not None:
                    src_client.read_namespaced_pod(name, namespace)
                else:
                # checking the pod after 1 second until the pod is deleted
                    self.core_api.read_namespaced_pod(name, namespace)
                time.sleep(2)
        except ApiException as e:
            logger.info('Pod "{}" deleted.'.format(name))

        return True

    def delete_service(self, name: str,  migration: bool = False,
                    src_client: CoreV1Api = None, namespace: str = None) -> bool:

        """Delete a specific Service

        :param name: str
            name of Service

        :param namespace: str
            namespace of Service
        """
        if namespace is None:
            namespace = self.namespace

        try:
            logger.info("Trying to delete service {service}")
            if migration is True and src_client is not None:
                src_client.delete_namespaced_service(name, namespace)
            else: 
                self.core_api.delete_namespaced_service(name, namespace)
        except ApiException as e:
            logger.error(e)

        try:
            while True:
                if migration is True and src_client is not None:
                    src_client.read_namespaced_service(name, namespace)
                else:
                    self.core_api.read_namespaced_service(name, namespace)
                time.sleep(2)
        except ApiException as e:
            logger.info("Service {name} successfully deleted")

        return True

    def move_pod(self, previousPod: V1Pod,
                 previousService: V1Service,
                 limits: dict,
                 to_node_name: str,
                 to_node_id: int,
                 src_context= str,
                 namespace: str = None):
        """Move a Pod from a node to another one
            we will create a new instance from a service with different name and remove previous one

        :param previousPod: V1Pod
            previous Pod

        :param previousService: V1Service
            previous Service

        :param to_node_name: str
            name of the node which you want to start the pod inprallel

        :param to_node_id: int
            id of the node which you want to start the pod in

        :param namespace: str
            namespace of Pod
        """
        # TODO: we should implement it
        #  get information of a Pod and change name that and create new one

        # TODO: if new limits are not provided, yse the old one

        if namespace is None:
            namespace = self.namespace

        self.migration = True

        source_client = self.initialize_client(src_context)
        #desitnation_client = self.initialize_client(destination_context)

        try:
            previousPodName = get_pod_name(previousPod)
        except ApiException as e:
            previousPodName = previousPod.metadata.name
            logger.error("Error getting existing pod name. {e}")

        # make sure this pod does not exist already on the destination node
        # TODO: Update this for scaling on same node
        if to_node_name == previousPod.spec.node_name and previousPod.spec.containers[0].resources.limits == limits:
            logger.info(f"[Migration] Pod {previousPodName} already exist in node"
                        f" {previousPod.spec.node_name}.")

            return previousPod, previousService, previousService.status.load_balancer.ingress[0].ip

        logger.info(f"[Migration] Pod {previousPodName} migration from node {previousPod.spec.node_name} to "
                    f"node {to_node_name} has started...")

        # extract service name from the pod name
        # fromat s[1-9]*-(a-z)*
        service_id = int(previousPodName.split('-')[0].split('s')[1].split('n')[0])
        # New Name in reference to new node
        newName = generate_random_service_name(service_id=service_id,
                                                    node_id=to_node_id)
        
        # updating labels from previousPod to newName

        metadata: V1ObjectMeta = V1ObjectMeta(
            name=newName,
            labels={**previousPod.metadata.labels, "svc": newName}
            )

        container = previousPod.spec.containers[0]
        previousPod.spec.containers[0].name = newName
        container.resources.requests = limits
        container.resources.limits = limits
        previousPod.spec.containers[0].resources.requests = limits
        previousPod.spec.containers[0].resources.limits = limits

        # The pod name will be same. Only changing service Name. We can also create a new name for the pod
        # Spec contains the image name. Make sure you have provided corrct one

        podSpec: V1PodSpec = previousPod.spec
        podSpec.hostname = newName
        podSpec.node_name = to_node_name

        newPod = V1Pod(
            api_version=previousPod.api_version,
            metadata=metadata,
            spec=podSpec
        )

        logger.info(f'[Migration] Creating pod {previousPodName} in node {newPod.spec.node_name}')
        createP = self.create_pod(newPod, namespace)

        if createP is None:
            logger.warn("[Migration] Creating Pod faced an issue...")
            return None, previousService, previousService.status.load_balancer.ingress[0].ip

        logger.info(f'[Migration] Creating service {newName}')
        serviceSpec: V1ServiceSpec = previousService.spec
        serviceSpec.selector = {
            "svc": newName
        }
        serviceSpec.cluster_ip = None
        serviceSpec.cluster_i_ps = []
        serviceSpec.ports[0].node_port = None

        newService = V1Service(
            api_version=previousService.api_version,
            metadata=metadata,
            spec=serviceSpec  # look into why don't we use the 'serviceSpec'
        )

        createS, new_ServiceIP = self.create_service(service=newService, namespace=namespace)

        if createS is None:
            logger.warn(f"[Migration] Service {newName} creation failed due to an issue, exiting...")
            return createP, None, None
        
        logger.info(f"[Migration] Waiting for external IP assignment to service {newName}...")
        
        if new_ServiceIP:
            logger.info(f'New Service {newName} IP: {new_ServiceIP}')

        # So far, Pod and associated service are created.

        logger.info(f"[Migration] Deleting previous pod {previousPodName} from node {previousPod.spec.node_name}")
        self.delete_pod(name=previousPod.metadata.name, migration=self.migration, src_client=source_client, namespace=namespace)

        # TODO: This piece of code is different from the Smart Kube for the service name
        logger.info(f"[Migration] Deleting previous service {previousService.metadata.name}")
        self.delete_service(previousService.metadata.name, self.migration, source_client, namespace)

        logger.info("[Migration] Migration Done!")
        return createP, createS, new_ServiceIP

    def clean(self, namespace: str = None):
        """Clean all Pods of a namespace

            :param namespace: str

            1. Delete all the pods
            2. delete the namespace
        """
        if namespace is None:
            logger.info(f"No namespace provided, So cleaning the default namespace {namespace}!")

        logger.info("Terminating Pods...")

        # delete all pods in given namespace
        self.core_api.delete_collection_namespaced_pod(namespace)

        logger.info("Looking for namespace '{}'".format(
            namespace
        ))

        try:
            self.core_api.read_namespace(namespace)
        except ApiException as e:
            logger.error(f'Namespace {namespace} does not exist')
            return True

        # remove namespace
        logger.info("Removing namespace '{}'".format(namespace))
        self.core_api.delete_namespace(namespace)

        while True:
            time.sleep(1)
            try:
                self.core_api.read_namespace(namespace)
            except ApiException as e:
                logger.warn("namespace '{}' removed.".format(namespace))
                return True


class KubeCluster:
    """Kubernetes Cluster"""

    def __init__(
            self,
            context: str,
            config_file: str = None,
            namespace: str = None,
            utilization_server_image: str = None,
            cleaning_after_exiting: bool = False
    ):
        """Kubnernetes Cluster Constructor

        :param config_file: str (default: ~/.kube/config)
            address of config file

        :param namespace: str (default: 'consolidation')
            using namespace

        :param workloads_path: str (default: './workloads.pickle')
            path of workloads cluster

        :param cluster_path: str (default: './cluster.pickle')
            path of workloads cluster

        :param utilization_server_image: str
            using this image to start utilization server (default: 'r0ot/sdghafouri/utilization-server-smart-scheduler')

        :param cleaning_after_exiting: bool (default: False)
            clean the cluster after exiting

        :param
        """

        if config_file is None:
            # set default value for config_file
            config_file = '~/.kube/config'

        if namespace is None:
            # set default value for namespace
            namespace = 'prokube'

        if utilization_server_image is None:
            # set default value for Utilization image
            utilization_server_image = 'sdghafouri/utilization-server-smart-scheduler'

        # define namespace
        self.namespace = namespace

        # define the default config file path
        self.config_file: str = config_file

        # # define workloads path
        # self.workloads_path: str = workloads_path
        #
        # # define cluster path
        # self.cluster_path: str = cluster_path
        #
        # # using auxiliary server
        # self.using_auxiliary_server: bool = using_auxiliary_server

        # using utilization server image
        self.utilization_server_image: str = utilization_server_image

        # set config file for client
        config.load_kube_config(context=context)

        # define client interface (general api)
        self.core_api = CoreV1Api()

        # define custom object API for metrics monitoring
        self.obj_api = CustomObjectsApi()

        self.apps_api = AppsV1Api()

        # define Monitor class Object
        self.monitor = Monitor(
            self.core_api,
            self.obj_api,
            self.apps_api,
            self.namespace
        )

        self.cleaning_after_exiting = cleaning_after_exiting

        # Choose A Node
        nodes = self.monitor.get_nodes()

        # define action interface
        self.action = Action(
            self.core_api,
            self.obj_api,
            self.apps_api,
            self.namespace,
            self.utilization_server_image,
            nodes[0],
            config_file,
            self.cleaning_after_exiting
        )
