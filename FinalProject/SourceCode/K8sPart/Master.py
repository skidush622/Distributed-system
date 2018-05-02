#!/usr/bin/python
# -*- coding: utf-8 -*-

from os import path

import yaml

from kubernetes import client, config


def create_deployment_using_yaml(yaml_file):
	config.load_kube_config()
	with open(path.join(path.dirname(__file__), yaml_file)) as f:
		dep = yaml.load(f)
		k8s_beta = client.ExtensionsV1beta1Api()
		resp = k8s_beta.create_namespaced_deployment(
			body=dep, namespace="default")
		print("Deployment created using %s. status='%s'" % (yaml_file, str(resp.status)))


def create_service(service_name, app_name, port, node_port):
	# Load config from default location
	config.load_kube_config()

	# Create API endpoint instance
	api_instance = client.CoreV1Api()

	# Create API resource instances
	service = client.V1Service()

	#  required Service fields
	service.api_version = "v1"
	service.kind = 'Service'
	service.metadata = client.V1ObjectMeta(name=service_name)

	# Provide Service .spec description
	spec = client.V1ServiceSpec()
	spec.selector = {"app": app_name}
	spec.type = 'NodePort'
	spec.ports = [client.V1ServicePort(protocol='TCP', node_port=node_port, port=port)]
	service.spec = spec

	# create service
	api_instance.create_namespaced_service(namespace="default", body=service)


def create_deployment(deployment_name, pod_label, container_name, image_name, container_port):
	# Load config from default location
	config.load_kube_config()
	extension = client.ExtensionsV1beta1Api()

	container = client.V1Container(
		name=container_name,
		image=image_name,
		ports=[client.V1ContainerPort(container_port=container_port)])

	# Create and configurate a spec section
	template = client.V1PodTemplateSpec(
		metadata=client.V1ObjectMeta(labels={"app": pod_label}),
		spec=client.V1PodSpec(containers=[container]))

	# Create the specification of deployment
	spec = client.ExtensionsV1beta1DeploymentSpec(
		replicas=3,
		template=template)

	# Instantiate the deployment object
	deployment = client.ExtensionsV1beta1Deployment(
		api_version="extensions/v1beta1",
		kind="Deployment",
		metadata=client.V1ObjectMeta(name=deployment_name),
		spec=spec)

	# create deployment
	extension.create_namespaced_deployment(namespace="default", body=deployment)


def create_initial_operator_deployments():
	base_port = 2300
	for i in range(3):
		deployment_name = 'operator' + str(i + 1)
		pod_label = 'operator' + str(i + 1)
		container_name = 'operator' + str(i + 1)
		image_name = 'zhuangweikang/operator_image:0.01'
		container_port = base_port + i + 1
		create_deployment(deployment_name, pod_label, container_name, image_name, container_port)
		print('Create operator deployment %s ' % deployment_name)


if __name__ == '__main__':
	# create deployment for ingress operator
	ingress_yaml = './OperatorsDeployment/ingress-deployment.yaml'
	create_deployment_using_yaml(ingress_yaml)

	# create service for ingress operator
	create_service('ingress', 'ingress', 2340, 30010)

	# create deployment for output operator
	output_yaml = './OperatorsDeployment/output-deployment.yaml'
	create_deployment_using_yaml(output_yaml)

	# create deployment for egress operator
	egress_yaml = './OperatorsDeployment/egress-deployment.yaml'
	create_deployment_using_yaml(egress_yaml)

	# create initial operator deployments
	create_initial_operator_deployments()



