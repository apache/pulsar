CLUSTER_NAME=pulsar-gke
ZONE=us-west1-a

launch-gke-cluster:
	gcloud container clusters create ${CLUSTER_NAME} \
		--zone=${ZONE} \
		--machine-type=n1-standard-8 \
		--num-nodes=3 \
		--local-ssd-count=2 \
		--cluster-version=1.6.4

delete-cluster:
	gcloud container clusters delete ${CLUSTER_NAME}

launch-admin-pod:
	kubectl exec pulsar-admin -it -- bash

launch-bookies:
	kubectl apply -f bookie.yaml

launch-broker:
	kubectl apply -f broker.yaml

launch-monitoring:
	kubectl apply -f monitoring.yaml

launch-zookeeper:
	kubectl apply -f zookeeper.yaml

initialize-cluster-metadata:
	kubectl exec -it zk-0 -- \
		bin/pulsar initialize-cluster-metadata \
		--cluster ${CLUSTER_NAME} \
		--zookeeper zookeeper \
		--global-zookeeper zookeeper \
		--web-service-url http://broker.default.svc.cluster.local:8080/ \
		--broker-service-url pulsar://broker.default.svc.cluster.local:6650/
