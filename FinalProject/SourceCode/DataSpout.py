# Project Name: An Elastic Real-Time Stream Processing System Based on K8s
#
# Team Member: Zhuangwei Kang, Manyao Peng, Yingqi, Li, Minhui, Zhou
#
# File Name: DataSpout.py
# NOTE: This file is used to build a K8s cluster.
#
from kubernetes import client, config

DEPLOYMENT_NAME = "data_sources-deployment"


def create_deployment_object():
    # Configureate Pod template container
    container1 = client.V1Container(
        name="data_spout1",
        image="ubuntu:16.04",
        ports=[client.V1ContainerPort(container_port=5556)])
    container2 = client.V1Container(
        name="data_spout2",
        image="ubuntu:16.04",
        ports=[client.V1ContainerPort(container_port=5557)])
    container3 = client.V1Container(
        name="data_spout3",
        image="ubuntu:16.04",
        ports=[client.V1ContainerPort(container_port=5558)])
    container4 = client.V1Container(
        name="data_spout4",
        image="ubuntu:16.04",
        ports=[client.V1ContainerPort(container_port=5559)])
    container5 = client.V1Container(
        name="data_spout5",
        image="ubuntu:16.04",
        ports=[client.V1ContainerPort(container_port=6000)])
    # Create and configurate a spec section
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": "data_spout"}),
        spec=client.V1PodSpec(containers=[container1, container2, container3, container4, container5]))
    # Create the specification of deployment
    spec = client.ExtensionsV1beta1DeploymentSpec(
        replicas=3,
        template=template)
    # Instantiate the deployment object
    deployment = client.ExtensionsV1beta1Deployment(
        api_version="extensions/v1beta1",
        kind="Deployment",
        metadata=client.V1ObjectMeta(name=DEPLOYMENT_NAME),
        spec=spec)

    return deployment


def create_deployment(api_instance, deployment):
    # Create deployement
    api_response = api_instance.create_namespaced_deployment(
        body=deployment,
        namespace="default")
    print("Deployment created. status='%s'" % str(api_response.status))


def main():
    config.load_kube_config()
    extensions_v1beta1 = client.ExtensionsV1beta1Api()
    deployment = create_deployment_object()
    create_deployment(extensions_v1beta1, deployment)


if __name__ == '__main__':
    main()