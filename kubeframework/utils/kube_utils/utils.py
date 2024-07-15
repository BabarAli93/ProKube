## it will implement basic functions related to Nod3, pod and services
# Get me nodes capacity, node name, pod name, service name, POD YAML and Service YAML
# Do I need other things. If needed, extend this file related to Kubernetes

from string import ascii_lowercase
from kubernetes.client import (
    V1Pod,
    V1Node,
    V1EnvVar,
    V1Deployment,
    V1DeploymentSpec,
    V1LabelSelector,
    V1PodTemplateSpec,
    V1PodSpec,
    V1Container,
    V1ObjectMeta,
    V1ResourceRequirements,
    V1Service, V1ServiceSpec, V1ServicePort,
)
import random
import uuid


def get_node_capacity(node: V1Node) -> dict:
    # it will return node capacity in dictionary format
    """Get Capacity of a Node

        :param node: V1Node
            node object

        :return dict
        """
    return node.status.capacity

def get_node_availability(node: V1Node) -> dict:
    
    """Get available resources of a Node - remaining resouurces to be alloacted

        :param node: V1Node
            node object

        :return dict
        """
    return node.status.allocatable


def get_node_name(node: V1Node) -> str:
    # this function will return node name in string
    """Get name of a Node

        :param node: V1Node
            node object

        :return str"""

    return node.metadata.name


def get_pod_name(pod: V1Pod, source='container') -> str:
    """Get name of a Pod

        :param pod: V1Pod
            pod object

        :param source: str (default: container)
            valid sources: container, metadata

        :return str
        """

    if source == 'container':
        containers = pod.spec.containers
        if len(containers) > 0:
            return containers[0].name

    return pod.metadata.name


def get_service_name(service: V1Service) -> str:
    """Get name of a Service

        :param service: V1Service
            service object

        :return str
        """

    return service.metadata.name


def generate_random_service_name(
        service_id: int = 0, node_id=0, size: int = 10) -> str:
    """Generate a random string

        :param node_id:
        :param service_id:
        :param size: int (default: 10)
            size of generated string

        format: s+service_id+n+node_id+(some random string)
        :return str

        sample:  s0n0-abcdefghij

        This will be needed to migrate or create services.
        """
    short_uuid = str(uuid.uuid4())[:8]
    name = 's' + str(service_id) + 'n' + str(node_id) + \
           '-' + short_uuid + '-' + ''.join(random.choices(ascii_lowercase, k=size))

    return name


def construct_deployment(
        name: str,
        image: str,
        node_name: str = None,
        labels: dict = None,
        namespace: str = None,
        request_mem: str = None,
        request_cpu: str = None,
        limit_mem: str = None,
        limit_cpu: str = None,
        env: dict = None
        
)-> V1Deployment:
    """This function aims to create a Deployment YAML/ Body of One pod per 
    Deployment in the provided node"""

    if labels is None:
        # set default value for labels
        labels = dict(
            env='park',
            svc=name
        )

    if namespace is None:
        # set default value for namespace
        namespace = 'prokube'

    if env is None:
        env = [
        {
            "name": "RAM",
            "value": "80M"
        }
    ]


    limits, requests = dict(), dict()

    if request_mem is not None:
        requests.update(memory=request_mem)

    if request_cpu is not None:
        requests.update(cpu=request_cpu)

    if limit_mem is not None:
        limits.update(memory=limit_mem)

    if limit_cpu is not None:
        limits.update(cpu=limit_cpu)

    deployment = V1Deployment(
        api_version = "apps/v1",
        kind = "Deployment",
        metadata = V1ObjectMeta(
            name=name,
            namespace=namespace,
            labels=labels,
        ),

        spec = V1DeploymentSpec(
            replicas=1,
            selector = V1LabelSelector(
                match_labels=labels
            ),
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(labels=labels),
                spec=V1PodSpec(
                    hostname=name,
                    containers=[
                        V1Container(
                            name=name,
                            image=image,
                            env=env,
                            resources=V1ResourceRequirements(
                                requests=limits,
                                limits=limits
                            )
                        )
                    ],
                    termination_grace_period_seconds=0,
                    restart_policy="Always",
                    node_name=node_name
                )
            )
        )
    )

    return deployment


def construct_pod(
        name: str,
        image: str,
        node_name: str = None,
        labels: dict = None,
        namespace: str = None,
        request_mem: str = None,
        request_cpu: str = None,
        limit_mem: str = None,
        limit_cpu: str = None,
        env: dict = None
) -> V1Pod:
    # node_name: str = None (Not added at the moment)
    """ This function will construct the YAML a.k.a body of a pod

    """

    if labels is None:
        # set default value for labels
        labels = dict(
            env='park',
            svc=name
        )

    if namespace is None:
        # set default value for namespace
        namespace = 'prokube'

    if env is None:
        env = dict(
            RAM="80M"
        )
    # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1EnvVar.md
    # To set environment variable, we need to set the name and the value of that variable
    # prepare environment variables for Pod
    env = [V1EnvVar(name=key, value=value) for key, value in env.items()]

    limits, requests = dict(), dict()

    if request_mem is not None:
        requests.update(memory=request_mem)

    if request_cpu is not None:
        requests.update(cpu=request_cpu)

    if limit_mem is not None:
        limits.update(memory=limit_mem)

    if limit_cpu is not None:
        limits.update(cpu=limit_cpu)

    pod = V1Pod(
        api_version='v1',
        kind='Pod',
        metadata=V1ObjectMeta(
            name=name,
            namespace=namespace,
            labels=labels,
        ),
        spec=V1PodSpec(
            hostname=name,
            containers=[
                V1Container(
                    name=name,
                    image=image,
                    env=env,
                    image_pull_policy='Always',
                    resources=V1ResourceRequirements(
                        requests=limits,
                        limits=limits
                    )
                )
            ],
            termination_grace_period_seconds=0,
            restart_policy="Never",
            # we will update this later when we get the scheduling decisions
            node_name=node_name
        )
    )

    return pod


def construct_service(
        name: str,
        namespace: str = None,
        labels: dict = None,
        portName: str = None,
        port: int = None,
        targetPort: int = None,
        portProtocol: str = None
):
    # Not sure what will be the port protocol on 5000

    if namespace is None:
        # set default value for namespace
        namespace = 'prokube'

    if labels is None:
        labels = dict(
            env='park',
            svc=name
        )

    if portName is None:
        portName = 'user-listening'

    if port is None:
        port = 5000

    if targetPort is None:
        targetPort = 5000

    if portProtocol is None:
        portProtocol = 'TCP'

    service = V1Service(
        api_version='v1',
        kind='Service',
        metadata=V1ObjectMeta(
            name=name,
            namespace=namespace,
            labels=labels
        ),
        spec=V1ServiceSpec(
            type="LoadBalancer",
            ports=[
                V1ServicePort(
                    name=portName, protocol=portProtocol, port=port, target_port=targetPort
                )
            ],
            selector=dict(
                svc=name
            )
        )
    )

    return service

def mapper(function, data: list, conv=None):
    """Mapping

    :param function: func
        mapper function

    :param data: list
        apply mapper function on each item of data

    :param conv: type (default: list)
        convert map function into conv
    """

    if conv is None:
        # set default value for conv
        conv = list

    return conv(map(function, data))