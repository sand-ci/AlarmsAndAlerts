
kubectl create -f ./namespace.yaml

kubectl delete secret  config -n aaas 
kubectl create secret generic config -n aaas --from-file=secrets/config.json

kubectl create -f ./ps-indexing.yaml
kubectl create -f ./ps-packetloss.yaml
kubectl create -f ./ps-clock-corrections.yaml
