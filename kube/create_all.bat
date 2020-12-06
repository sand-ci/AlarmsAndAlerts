
kubectl create -f ./namespace.yaml

kubectl delete secret  config -n aaas 
kubectl create secret generic config -n aaas --from-file=secrets/config.json

@REM kubectl create -f  ./secrets/mailgun-secret.yaml

kubectl create -f ./ps-indexing.yaml
@REM kubectl create -f ./sending-mails.yaml
